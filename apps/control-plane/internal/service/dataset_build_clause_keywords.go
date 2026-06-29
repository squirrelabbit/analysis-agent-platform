package service

import (
	"context"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/registry"
)

// BuildClauseKeywords — silverone 2026-06-10. clause_label 결과에서 Kiwi 명사
// 키워드를 추출해 long-format clause_keywords artifact를 만든다 (수동 build).
//
// precondition: clause_label_ref 존재(= clause_label ready). LLOA 호출 없는
// 결정론적 단계라 doc_genuineness/genuineness 필터는 쓰지 않는다.
//
// metadata: clause_keywords_status / clause_keywords_ref / clause_keywords_summary /
// clause_keywords_completed_at 등을 저장. analyze는 clause_keywords_ref가 있으면
// optional reserved table로 주입한다(없으면 기존 흐름 그대로).
func (s *DatasetService) BuildClauseKeywords(projectID, datasetID, datasetVersionID string, input domain.DatasetClauseKeywordsBuildRequest) (domain.DatasetVersion, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}

	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	clauseRef := strings.TrimSpace(metadataString(version.Metadata, "clause_label_ref", ""))
	if clauseRef == "" {
		clauseRef = strings.TrimSpace(metadataString(version.Metadata, "clause_label_uri", ""))
	}
	if clauseRef == "" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "clause_label artifact not ready — clause_keywords requires clause_label first"}
	}

	outputPath := s.datasetArtifactPathOrFallback(version, "clause_keywords", "clause_keywords.jsonl")
	progressPath := outputPath + ".progress.json"

	version.Metadata["clause_keywords_status"] = "running"
	delete(version.Metadata, "clause_keywords_cancelled") // 재실행은 처음부터
	version.Metadata["clause_keywords_uri"] = outputPath
	version.Metadata["clause_keywords_ref"] = outputPath
	version.Metadata["clause_keywords_progress_ref"] = progressPath
	version.Metadata["clause_keywords_input_source"] = "clause_label"
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	payload := map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"clause_label_ref":   clauseRef,
		"output_path":        outputPath,
		"progress_path":      progressPath,
	}
	if input.KeywordMinLen != nil && *input.KeywordMinLen > 0 {
		payload["keyword_min_len"] = *input.KeywordMinLen
	}

	// 키워드 정제 사전 baked-in (silverone 2026-06-25, Phase 2). 활성 block/synonym
	// 규칙을 payload로 넘겨 extractor가 추출 시점에 제외/병합하게 한다 — 재빌드하면
	// artifact 자체가 정제본이라 보고서/analyze에도 반영된다(키워드 뷰만 overlay하던
	// Phase 1과 달리). clause_label의 taxonomy_id/doc_genuineness inject 패턴과 동일.
	if rules, err := s.store.ListKeywordDictionaryRules(projectID, datasetID, true); err == nil && len(rules) > 0 {
		payload["keyword_dictionary_rules"] = rules
	}
	// 도메인 불용어 룰 — dataset.metadata override가 있으면 사용(없으면 worker 기본 festival-v1).
	if dataset, err := s.GetDataset(projectID, datasetID); err == nil {
		if name := strings.TrimSpace(metadataString(dataset.Metadata, "keyword_stopwords_rule_name", "")); name != "" {
			payload["keyword_stopwords_rule_name"] = name
		}
	}

	response, err := s.runWorkerTask(context.Background(), registry.TaskPathFor("dataset_clause_keywords"), payload)
	if err != nil {
		version.Metadata["clause_keywords_status"] = "failed"
		version.Metadata["clause_keywords_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	now := time.Now().UTC()
	artifact := response.Artifact
	if artifact == nil && len(response.Artifacts) > 0 {
		artifact = response.Artifacts[0]
	}
	keywordsRef := artifactString(artifact, "clause_keywords_ref")
	if keywordsRef == "" {
		keywordsRef = artifactString(artifact, "clause_keywords_uri")
	}
	if keywordsRef == "" {
		keywordsRef = outputPath
	}
	version.Metadata = mergeStringAny(version.Metadata, map[string]any{
		"clause_keywords_status":       "ready",
		"clause_keywords_uri":          keywordsRef,
		"clause_keywords_ref":          keywordsRef,
		"clause_keywords_input_source": "clause_label",
		"clause_keywords_completed_at": now,
		"clause_keywords_notes":        response.Notes,
	})
	delete(version.Metadata, "clause_keywords_error")
	if summary, ok := artifact["summary"].(map[string]any); ok {
		version.Metadata["clause_keywords_summary"] = summary
		// 빌드 중단(silverone 2026-06-29) — 중단 시 부분 결과 미저장(status=cancelled + ref 제거).
		if c, _ := summary["cancelled"].(bool); c {
			version.Metadata["clause_keywords_cancelled"] = true
			version.Metadata["clause_keywords_status"] = "cancelled"
			delete(version.Metadata, "clause_keywords_ref")
			delete(version.Metadata, "clause_keywords_uri")
			removeArtifactFileQuietly(keywordsRef)
		} else {
			delete(version.Metadata, "clause_keywords_cancelled")
		}
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	if err := s.attachDatasetVersionArtifacts(&version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
}
