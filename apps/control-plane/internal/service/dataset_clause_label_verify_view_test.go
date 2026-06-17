package service

import (
	"os"
	"path/filepath"
	"testing"
)

// silverone 2026-06-17 — clause_label verify(ADR-028) read 경로 잠금. 검토 큐
// 필터(불일치=resolution!='agree' / 검토필요=needs_review)와 model A/B/judge
// snapshot 복원, resolution 분포 summary를 검증한다.
func setupClauseLabelVerifyFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "clause_label.verify.jsonl")
	lines := []string{
		`{"doc_id":"d1","clause":"c1","sentiment":"positive","aspect":"food","resolution":"agree","needs_review":false,"sentence_index":1,"chunk_index":0,"model_a_result":{"relevant":true,"sentiment":"positive","aspects":["food"]},"model_b_result":{"relevant":true,"sentiment":"positive","aspects":["food"]},"judge_result":null,"source":"verify"}`,
		`{"doc_id":"d1","clause":"c2","sentiment":"positive","aspect":"price","resolution":"union","needs_review":false,"sentence_index":2,"chunk_index":0,"model_a_result":{"relevant":true,"sentiment":"positive","aspects":["price"]},"model_b_result":{"relevant":true,"sentiment":"positive","aspects":["price","food"]},"judge_result":null,"source":"verify"}`,
		`{"doc_id":"d2","clause":"c3","sentiment":"negative","aspect":"service","resolution":"judge","needs_review":false,"sentence_index":1,"chunk_index":1,"model_a_result":{"relevant":true,"sentiment":"negative","aspects":["service"]},"model_b_result":{"relevant":true,"sentiment":"neutral","aspects":["food"]},"judge_result":{"relevant":true,"sentiment":"negative","aspects":["service"],"reason":"불만 맥락"},"source":"verify"}`,
		`{"doc_id":"d2","clause":"c4","sentiment":"neutral","aspect":"etc","resolution":"partial_classify","needs_review":true,"sentence_index":2,"chunk_index":1,"model_a_result":{"relevant":true,"sentiment":"neutral","aspects":["etc"]},"model_b_result":null,"judge_result":null,"source":"verify"}`,
	}
	if err := os.WriteFile(path, []byte(joinLines(lines)), 0o644); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}
	return path
}

func TestLoadClauseLabelVerifyArtifact_NoFilter(t *testing.T) {
	path := setupClauseLabelVerifyFixture(t)
	summary, _, total, items, err := loadClauseLabelVerifyArtifact(path, 10, 0, "", "", false, false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if total != 4 || len(items) != 4 {
		t.Fatalf("total=%d items=%d, want 4/4", total, len(items))
	}
	res, ok := summary["resolution"].(map[string]int)
	if !ok {
		t.Fatalf("summary.resolution missing/typed: %T", summary["resolution"])
	}
	for k, want := range map[string]int{"agree": 1, "union": 1, "judge": 1, "partial_classify": 1} {
		if res[k] != want {
			t.Fatalf("resolution[%s]=%d, want %d", k, res[k], want)
		}
	}

	// model A/B/judge snapshot 복원 확인 (judge 행).
	var judgeRow map[string]any
	for _, it := range items {
		if it["resolution"] == "judge" {
			judgeRow = it
		}
	}
	if judgeRow == nil {
		t.Fatal("judge row not found")
	}
	ma, _ := judgeRow["model_a_result"].(map[string]any)
	if ma == nil || ma["sentiment"] != "negative" {
		t.Fatalf("model_a_result = %v, want negative", judgeRow["model_a_result"])
	}
	jr, _ := judgeRow["judge_result"].(map[string]any)
	if jr == nil || jr["reason"] != "불만 맥락" {
		t.Fatalf("judge_result = %v, want reason 불만 맥락", judgeRow["judge_result"])
	}
	if sidx, ok := judgeRow["sentence_index"].(int); !ok || sidx != 1 {
		t.Fatalf("sentence_index = %v (%T), want int 1", judgeRow["sentence_index"], judgeRow["sentence_index"])
	}

	// 합의 행은 judge_result nil.
	for _, it := range items {
		if it["resolution"] == "agree" && it["judge_result"] != nil {
			t.Fatalf("agree row judge_result must be nil, got %v", it["judge_result"])
		}
	}
}

func TestLoadClauseLabelVerifyArtifact_DisagreementFilter(t *testing.T) {
	path := setupClauseLabelVerifyFixture(t)
	summary, _, total, items, err := loadClauseLabelVerifyArtifact(path, 10, 0, "", "", true, false)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// resolution != 'agree' → union/judge/partial = 3.
	if total != 3 || len(items) != 3 {
		t.Fatalf("disagreement total=%d items=%d, want 3/3", total, len(items))
	}
	for _, it := range items {
		if it["resolution"] == "agree" {
			t.Fatalf("agree row must be filtered out")
		}
	}
	// summary는 전체 분포 유지.
	if summary["total"] != 4 {
		t.Fatalf("summary.total must stay 4, got %v", summary["total"])
	}
}

func TestLoadClauseLabelVerifyArtifact_NeedsReviewFilter(t *testing.T) {
	path := setupClauseLabelVerifyFixture(t)
	_, _, total, items, err := loadClauseLabelVerifyArtifact(path, 10, 0, "", "", false, true)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("needs_review total=%d items=%d, want 1/1", total, len(items))
	}
	if items[0]["needs_review"] != true {
		t.Fatalf("filtered row needs_review = %v, want true", items[0]["needs_review"])
	}
	// partial_classify 행은 model_b 미분류 → nil.
	if items[0]["model_b_result"] != nil {
		t.Fatalf("partial row model_b_result must be nil, got %v", items[0]["model_b_result"])
	}
}
