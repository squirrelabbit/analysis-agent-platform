package service

import (
	"os"
	"path/filepath"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

// 회귀 잠금 (2026-06-15) — clause_keywords view가 진행률(progress)을 반환하지
// 못하던 버그. buildJobMetadataPrefix에 clause_keywords case가 빠져 있어
// enrichViewWithJob이 progress 파일 경로를 못 찾고 view.Progress=nil로 두었다
// (화면에서 100% 진행률 미표시). worker는 progress.json에 100%를 정상 기록.

// buildJobMetadataPrefix가 4개 build type 모두 prefix를 돌려줘야 한다.
// clause_keywords가 빠지면 progress 로드가 통째로 누락된다.
func TestBuildJobMetadataPrefixCoversClauseKeywords(t *testing.T) {
	cases := map[string]string{
		datasetBuildTypeClean:          "clean",
		datasetBuildTypeDocGenuineness: "doc_genuineness",
		datasetBuildTypeClauseLabel:    "clause_label",
		datasetBuildTypeClauseKeywords: "clause_keywords",
	}
	for buildType, want := range cases {
		if got := buildJobMetadataPrefix(buildType); got != want {
			t.Fatalf("buildJobMetadataPrefix(%q) = %q, want %q", buildType, got, want)
		}
	}
}

// enrichViewWithJob이 clause_keywords의 progress.json을 읽어 view.Progress를
// 채우는지 잠금 (end-to-end로 prefix → 메타 키 → 파일 로드 경로 검증).
func TestEnrichViewWithJobLoadsClauseKeywordsProgress(t *testing.T) {
	progressPath := filepath.Join(t.TempDir(), "clause_keywords.jsonl.progress.json")
	body := `{"percent":100.0,"processed_rows":5529,"total_rows":5529,"elapsed_seconds":7.08,"eta_seconds":null,"message":"clause_keywords completed","updated_at":"2026-06-15T00:00:00.000000+00:00"}`
	if err := os.WriteFile(progressPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write progress fixture: %v", err)
	}

	metadata := map[string]any{
		// BuildClauseKeywords가 실제로 쓰는 메타 키.
		"clause_keywords_progress_ref": progressPath,
	}
	var view domain.DatasetArtifactView
	enrichViewWithJob(&view, nil, metadata, datasetBuildTypeClauseKeywords)

	if view.Progress == nil {
		t.Fatal("view.Progress is nil — clause_keywords 진행률이 응답에서 누락됨 (회귀)")
	}
	if view.Progress.Percent != 100.0 {
		t.Fatalf("progress.Percent = %v, want 100.0", view.Progress.Percent)
	}
	if view.Progress.ProcessedRows != 5529 || view.Progress.TotalRows != 5529 {
		t.Fatalf("progress rows = %d/%d, want 5529/5529",
			view.Progress.ProcessedRows, view.Progress.TotalRows)
	}
}
