package service

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// verify artifact view 잠금 (ADR-026, step 4b) — final_label을 effective label로
// 노출하고 needs_review/resolution/judge_result/model_*_result를 살린다.

func TestGetDocGenuinenessViewVerify(t *testing.T) {
	repo := store.NewMemoryStore()
	svc := NewDatasetService(repo, "", t.TempDir(), t.TempDir())
	if err := repo.SaveProject(domain.Project{ProjectID: "p1", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("project: %v", err)
	}
	if err := repo.SaveDataset(domain.Dataset{DatasetID: "d1", ProjectID: "p1", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("dataset: %v", err)
	}
	dir := t.TempDir()
	ref := filepath.Join(dir, "doc_genuineness.verify.jsonl")
	lines := `{"doc_id":"d1","model_a":"ma","model_a_result":{"genuineness":"genuine_review","reason":"ra"},"model_b":"mb","model_b_result":{"genuineness":"genuine_review","reason":"rb"},"is_disagreement":false,"judge_required":false,"judge_result":null,"final_label":"genuine_review","resolution":"model_agreement","needs_review":false,"prompt_version":"v1","source":"verify"}
{"doc_id":"d2","model_a":"ma","model_a_result":{"genuineness":"genuine_review","reason":"ra"},"model_b":"mb","model_b_result":{"genuineness":"non_review","reason":"rb"},"is_disagreement":true,"judge_required":true,"judge_result":{"decision":"accept_b","final_label":"non_review","confidence":0.7,"reason":"judge says ad","judge_model":"mj"},"final_label":"non_review","resolution":"judge_on_disagreement","needs_review":true,"prompt_version":"v1","source":"verify"}
`
	if err := os.WriteFile(ref, []byte(lines), 0o600); err != nil {
		t.Fatalf("write artifact: %v", err)
	}
	if err := repo.SaveDatasetVersion(domain.DatasetVersion{
		DatasetVersionID: "v1", DatasetID: "d1", ProjectID: "p1",
		Metadata: map[string]any{
			"doc_genuineness_mode":   "verify",
			"doc_genuineness_status": "ready",
			"doc_genuineness_ref":    ref,
			"doc_genuineness_summary": map[string]any{
				"mode": "verify", "agreement_count": 1, "disagreement_count": 1,
				"judge_count": 1, "review_count": 1,
				"models":  map[string]any{"a": "ma", "b": "mb", "judge": "mj"},
				"applied": map[string]any{"classify_models": []any{"ma", "mb"}, "judge_model": "mj", "prompt_version": "v1"},
			},
		},
	}); err != nil {
		t.Fatalf("version: %v", err)
	}

	view, err := svc.GetDocGenuinenessView("p1", "d1", "v1", 100, 0, "", false, false)
	if err != nil {
		t.Fatalf("GetDocGenuinenessView verify: %v", err)
	}
	if view.Summary["mode"] != "verify" {
		t.Fatalf("summary.mode=%v, want verify", view.Summary["mode"])
	}
	byID := map[string]map[string]any{}
	for _, it := range view.Items {
		byID[it["doc_id"].(string)] = it
	}
	d1, d2 := byID["d1"], byID["d2"]
	if d1 == nil || d2 == nil {
		t.Fatalf("items missing: %+v", view.Items)
	}
	// effective label = final_label, 호환 필드 genuineness에도 동일.
	if d1["genuineness"] != "genuine_review" || d1["final_label"] != "genuine_review" {
		t.Fatalf("d1 labels: %+v", d1)
	}
	if d2["genuineness"] != "non_review" {
		t.Fatalf("d2 effective label=%v, want non_review", d2["genuineness"])
	}
	if d1["needs_review"] != false || d2["needs_review"] != true {
		t.Fatalf("needs_review: d1=%v d2=%v", d1["needs_review"], d2["needs_review"])
	}
	if d1["is_disagreement"] != false || d2["is_disagreement"] != true {
		t.Fatalf("is_disagreement: d1=%v d2=%v", d1["is_disagreement"], d2["is_disagreement"])
	}
	// judge_result: d1 nil, d2 객체 with decision.
	if d1["judge_result"] != nil {
		t.Fatalf("d1 judge_result should be nil: %+v", d1["judge_result"])
	}
	jr, ok := d2["judge_result"].(map[string]any)
	if !ok || jr["decision"] != "accept_b" {
		t.Fatalf("d2 judge_result: %+v", d2["judge_result"])
	}
	// model 결과 노출.
	if ma, ok := d2["model_a_result"].(map[string]any); !ok || ma["genuineness"] != "genuine_review" {
		t.Fatalf("d2 model_a_result: %+v", d2["model_a_result"])
	}
	// verify 집계가 summary에 살아있는지.
	if view.Summary["disagreement_count"] == nil || view.Summary["judge_count"] == nil {
		t.Fatalf("verify counts missing in summary: %+v", view.Summary)
	}
	// applied에 classify/judge 모델.
	if view.Applied == nil || view.Applied["judge_model"] != "mj" {
		t.Fatalf("applied judge_model: %+v", view.Applied)
	}

	// 검토 큐 필터 — 불일치만 → d2만.
	disOnly, err := svc.GetDocGenuinenessView("p1", "d1", "v1", 100, 0, "", true, false)
	if err != nil {
		t.Fatalf("disagreement filter: %v", err)
	}
	if len(disOnly.Items) != 1 || disOnly.Items[0]["doc_id"] != "d2" {
		t.Fatalf("disagreement-only should return [d2], got %+v", disOnly.Items)
	}
	// needs_review만 → d2(needs_review=true)만.
	nr, err := svc.GetDocGenuinenessView("p1", "d1", "v1", 100, 0, "", false, true)
	if err != nil {
		t.Fatalf("needs_review filter: %v", err)
	}
	if len(nr.Items) != 1 || nr.Items[0]["doc_id"] != "d2" {
		t.Fatalf("needs_review-only should return [d2], got %+v", nr.Items)
	}
}
