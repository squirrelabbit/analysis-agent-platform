package service

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

// clause_label verify 모드 Go 배선 잠금 (ADR-028, 1b). doc_genuineness verify와
// 동일 패턴: verify면 classify_models 2개 + judge → 별도 artifact(.verify.jsonl) +
// clause_label_mode="verify". verifyFixture는 doc_genuineness verify 테스트와 공유.

func TestBuildClauseLabelVerifyPayload(t *testing.T) {
	svc, _ := verifyFixture(t)
	var payload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&payload)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"notes": []string{"done"},
			"artifact": map[string]any{
				"clause_label_ref": "/tmp/clause_label.verify.jsonl",
				"summary":          map[string]any{"mode": "verify"},
			},
		})
	}))
	defer server.Close()
	svc.pythonAIWorkerURL = server.URL

	got, err := svc.BuildClauseLabel("p1", "d1", "v1", domain.DatasetClauseLabelBuildRequest{
		Verify:             boolPtr(true),
		ClassifyModels:     []string{"wisenut/wise-lloa-max-v1.2.1", "wisenut/wise-lloa-ultra-v1.1.0"},
		JudgeModel:         strptr("wisenut/wise-lloa-ultra-v1.1.0"),
		IncludeGenuineness: []string{}, // opt-out → doc_genuineness ready 불필요
	})
	if err != nil {
		t.Fatalf("BuildClauseLabel verify: %v", err)
	}
	if payload["verify"] != true {
		t.Fatalf("payload.verify not set: %+v", payload["verify"])
	}
	cm, _ := payload["classify_models"].([]any)
	if len(cm) != 2 {
		t.Fatalf("classify_models not 2: %+v", payload["classify_models"])
	}
	if payload["judge_model"] != "wisenut/wise-lloa-ultra-v1.1.0" {
		t.Fatalf("judge_model: %+v", payload["judge_model"])
	}
	// verify면 단일 model_id는 안 넘긴다.
	if _, exists := payload["model_id"]; exists {
		t.Fatalf("verify should not pass model_id: %+v", payload["model_id"])
	}
	if got.Metadata["clause_label_mode"] != "verify" {
		t.Fatalf("mode metadata not verify: %+v", got.Metadata["clause_label_mode"])
	}
	if uri, _ := got.Metadata["clause_label_uri"].(string); !strings.HasSuffix(uri, "clause_label.verify.jsonl") {
		t.Fatalf("verify output path mismatch: %+v", got.Metadata["clause_label_uri"])
	}
}

func TestCreateClauseLabelJobVerifyValidation(t *testing.T) {
	svc, _ := verifyFixture(t)

	// classify_models 1개 → 거절.
	_, err := svc.CreateClauseLabelJob("p1", "d1", "v1", domain.DatasetClauseLabelBuildRequest{
		Verify: boolPtr(true), ClassifyModels: []string{"wisenut/wise-lloa-max-v1.2.1"},
	}, "api", "r1")
	var invalid ErrInvalidArgument
	if !errors.As(err, &invalid) {
		t.Fatalf("1 classify model should be ErrInvalidArgument, got %v", err)
	}

	// allowlist 밖 judge → 거절.
	_, err = svc.CreateClauseLabelJob("p1", "d1", "v1", domain.DatasetClauseLabelBuildRequest{
		Verify:         boolPtr(true),
		ClassifyModels: []string{"wisenut/wise-lloa-max-v1.2.1", "wisenut/wise-lloa-ultra-v1.1.0"},
		JudgeModel:     strptr("gpt-4o"),
	}, "api", "r1")
	if !errors.As(err, &invalid) {
		t.Fatalf("non-allowlist judge should be ErrInvalidArgument, got %v", err)
	}
}
