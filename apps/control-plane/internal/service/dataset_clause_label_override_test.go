package service

import (
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// 절 보정 overlay 합성 — items effective + summary(sentiment/aspect/cross) 재집계.
func TestApplyClauseLabelOverrides(t *testing.T) {
	view := domain.DatasetArtifactView{
		Items: []map[string]any{
			{"clause_id": "d1-1", "aspect": "service", "sentiment": "negative", "clause": "c2"},
		},
		Summary: map[string]any{
			"sentiment": map[string]int{"positive": 2, "negative": 1, "neutral": 1},
			"aspect":    map[string]int{"price": 2, "service": 1, "food": 1},
			"aspect_sentiment": map[string]any{
				"service": map[string]any{
					"total":     1,
					"sentiment": map[string]any{"negative": map[string]any{"count": 1, "percent": float64(100)}},
				},
				"price": map[string]any{
					"total":     2,
					"sentiment": map[string]any{"positive": map[string]any{"count": 2, "percent": float64(100)}},
				},
			},
		},
	}
	overrides := []domain.ClauseLabelOverride{{
		ClauseID: "d1-1", OriginalAspect: "service", OriginalSentiment: "negative",
		OverrideAspect: "price", OverrideSentiment: "positive", OverrideReason: "오분류 교정",
	}}
	applyClauseLabelOverrides(&view, overrides)

	it := view.Items[0]
	if it["aspect"] != "price" || it["sentiment"] != "positive" {
		t.Fatalf("effective 교체 잘못: %+v", it)
	}
	if it["original_aspect"] != "service" || it["original_sentiment"] != "negative" || it["is_overridden"] != true {
		t.Fatalf("original/override 필드 잘못: %+v", it)
	}

	sent := view.Summary["sentiment"].(map[string]int)
	if sent["negative"] != 0 || sent["positive"] != 3 {
		t.Fatalf("sentiment 재집계 잘못: %+v", sent)
	}
	asp := view.Summary["aspect"].(map[string]int)
	if asp["service"] != 0 || asp["price"] != 3 {
		t.Fatalf("aspect 재집계 잘못: %+v", asp)
	}
	cross := view.Summary["aspect_sentiment"].(map[string]any)
	pricePos := cross["price"].(map[string]any)["sentiment"].(map[string]any)["positive"].(map[string]any)
	if asInt(pricePos["count"]) != 3 {
		t.Fatalf("aspect_sentiment price.positive count = %v, want 3", pricePos["count"])
	}
	svcNeg := cross["service"].(map[string]any)["sentiment"].(map[string]any)["negative"].(map[string]any)
	if asInt(svcNeg["count"]) != 0 {
		t.Fatalf("aspect_sentiment service.negative count = %v, want 0", svcNeg["count"])
	}
	if view.Summary["override_count"] != 1 {
		t.Errorf("override_count = %v, want 1", view.Summary["override_count"])
	}
}

func TestClauseLabelOriginalForClause(t *testing.T) {
	path := setupClauseLabelFixture(t)
	// d1: c1(d1-0, price/positive), c2(d1-1, service/negative)
	aspect, sentiment, found, err := clauseLabelOriginalForClause(path, "d1-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if !found || aspect != "service" || sentiment != "negative" {
		t.Fatalf("d1-1 = %q/%q found=%v, want service/negative", aspect, sentiment, found)
	}
	_, _, found, err = clauseLabelOriginalForClause(path, "nope-9")
	if err != nil {
		t.Fatalf("missing lookup: %v", err)
	}
	if found {
		t.Fatal("없는 clause_id는 found=false여야 한다")
	}
}

func seedClauseVersion(t *testing.T, repo *store.MemoryStore, ref string) (string, string, string) {
	t.Helper()
	if err := repo.SaveProject(domain.Project{ProjectID: "p1", Name: "P", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("save project: %v", err)
	}
	if err := repo.SaveDataset(domain.Dataset{DatasetID: "d1", ProjectID: "p1", Name: "ds", CreatedAt: time.Now().UTC()}); err != nil {
		t.Fatalf("save dataset: %v", err)
	}
	if err := repo.SaveDatasetVersion(domain.DatasetVersion{
		DatasetVersionID: "v1", DatasetID: "d1", ProjectID: "p1",
		StorageURI: "/tmp/none.csv",
		Metadata: map[string]any{
			"clause_label_ref":    ref,
			"clause_label_status": "ready",
		},
	}); err != nil {
		t.Fatalf("save version: %v", err)
	}
	return "p1", "d1", "v1"
}

func TestSetAndDeleteClauseLabelOverrideFlow(t *testing.T) {
	repo := store.NewMemoryStore()
	svc := NewDatasetService(repo, "", t.TempDir(), t.TempDir())
	ref := setupClauseLabelFixture(t)
	pid, did, vid := seedClauseVersion(t, repo, ref)

	// d1-1 (service/negative) → sentiment만 positive로 (aspect는 원본 유지). 사유 미입력 → 기본값.
	ov, err := svc.SetClauseLabelOverride(pid, did, vid, "d1-1", domain.ClauseLabelOverrideRequest{
		Sentiment: "positive",
	})
	if err != nil {
		t.Fatalf("SetOverride: %v", err)
	}
	if ov.OriginalAspect != "service" || ov.OriginalSentiment != "negative" {
		t.Fatalf("original snapshot 잘못: %+v", ov)
	}
	if ov.OverrideAspect != "service" || ov.OverrideSentiment != "positive" {
		t.Fatalf("부분 보정(sentiment만) 잘못 — aspect 원본 유지여야: %+v", ov)
	}
	if ov.OverrideReason != "운영자 수동 수정" {
		t.Fatalf("사유 미입력 기본값 기대, got %q", ov.OverrideReason)
	}

	// aspect만 보정(재upsert).
	if _, err := svc.SetClauseLabelOverride(pid, did, vid, "d1-1", domain.ClauseLabelOverrideRequest{
		Aspect: "price", Reason: "aspect 교정",
	}); err != nil {
		t.Fatalf("SetOverride(aspect): %v", err)
	}
	list, _ := repo.ListClauseLabelOverrides(pid, vid)
	if len(list) != 1 || list[0].OverrideAspect != "price" || list[0].OverrideSentiment != "negative" {
		t.Fatalf("재보정 upsert(aspect만) 기대: %+v", list)
	}

	// DELETE 되돌리기.
	if err := svc.DeleteClauseLabelOverride(pid, did, vid, "d1-1"); err != nil {
		t.Fatalf("DeleteOverride: %v", err)
	}
	if err := svc.DeleteClauseLabelOverride(pid, did, vid, "d1-1"); err == nil {
		t.Fatal("이미 없는 보정 DELETE는 404여야")
	}

	// 없는 clause → 404.
	if _, err := svc.SetClauseLabelOverride(pid, did, vid, "nope-9", domain.ClauseLabelOverrideRequest{Sentiment: "positive"}); err == nil {
		t.Fatal("없는 clause_id set은 404여야")
	}
}

func TestSetClauseLabelOverrideValidation(t *testing.T) {
	repo := store.NewMemoryStore()
	svc := NewDatasetService(repo, "", t.TempDir(), t.TempDir())
	// 둘 다 비면 400 (version 조회 전에 거부).
	if _, err := svc.SetClauseLabelOverride("p1", "d1", "v1", "d1-0", domain.ClauseLabelOverrideRequest{}); err == nil {
		t.Fatal("aspect/sentiment 둘 다 비면 400")
	} else if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}
	// 잘못된 sentiment → 400.
	if _, err := svc.SetClauseLabelOverride("p1", "d1", "v1", "d1-0", domain.ClauseLabelOverrideRequest{Sentiment: "bogus"}); err == nil {
		t.Fatal("잘못된 sentiment는 400")
	} else if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}
}
