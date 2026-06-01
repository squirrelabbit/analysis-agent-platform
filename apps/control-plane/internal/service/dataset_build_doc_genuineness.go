package service

import (
	"context"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/registry"
)

// extractDocGenuinenessConfig — silverone 2026-05-22 (PR-α2). dataset.metadata
// 에서 doc_genuineness 설정을 정규화해서 Python worker payload에 inject할
// 형태로 반환한다.
//
// 정책:
//   - `subject_name` 누락 또는 공백 → ErrInvalidArgument (fail-loud, festival
//     prompt fallback 없음)
//   - `subject_aliases` 누락/잘못된 타입 → []
//   - `recruitment_keywords` 누락/잘못된 타입 → []
//   - `subject_type` 누락 → "generic"
//   - 항목 안의 빈/공백 문자열은 자동 drop
func extractDocGenuinenessConfig(metadata map[string]any) (map[string]any, error) {
	raw, _ := metadata["doc_genuineness"].(map[string]any)
	if raw == nil {
		return nil, ErrInvalidArgument{Message: "dataset.metadata.doc_genuineness.subject_name is required — set metadata.doc_genuineness in POST /projects/{pid}/datasets or PATCH /projects/{pid}/datasets/{did}/metadata before doc_genuineness build (no festival fallback)"}
	}
	subjectName := strings.TrimSpace(anyStringValue(raw["subject_name"]))
	if subjectName == "" {
		return nil, ErrInvalidArgument{Message: "dataset.metadata.doc_genuineness.subject_name is required — set metadata.doc_genuineness in POST /projects/{pid}/datasets or PATCH /projects/{pid}/datasets/{did}/metadata before doc_genuineness build (no festival fallback)"}
	}
	subjectType := strings.TrimSpace(anyStringValue(raw["subject_type"]))
	if subjectType == "" {
		subjectType = "generic"
	}
	aliases := normalizeStringList(anyStringList(raw["subject_aliases"]))
	if aliases == nil {
		aliases = []string{}
	}
	keywords := normalizeStringList(anyStringList(raw["recruitment_keywords"]))
	if keywords == nil {
		keywords = []string{}
	}
	return map[string]any{
		"subject_name":         subjectName,
		"subject_type":         subjectType,
		"subject_aliases":      aliases,
		"recruitment_keywords": keywords,
	}, nil
}

// BuildDocGenuineness — ADR-017 / 5/19 결정 5-step pipeline의 clean 직후
// doc-level 진성 분류. cleaned doc 단위로 LLOA에 호출해 3-tier
// (genuine_review / mixed / non_review) 라벨을 부여한다. clean ready
// precondition은 CreateDocGenuinenessJob에서 검사.
//
// silverone 2026-05-22 (PR-α2) — `dataset.metadata.doc_genuineness`를 읽어
// subject variables를 Python payload에 inject한다. `subject_name`이 누락되어
// 있으면 fail-loud — festival prompt fallback 없음.
func (s *DatasetService) BuildDocGenuineness(projectID, datasetID, datasetVersionID string, input domain.DatasetDocGenuinenessBuildRequest) (domain.DatasetVersion, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	docGenConfig, err := extractDocGenuinenessConfig(dataset.Metadata)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	cleanRef := cleanArtifactRef(version)
	if cleanRef == "" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "clean artifact ref missing — doc_genuineness requires clean ready"}
	}

	outputPath := s.datasetArtifactPathOrFallback(version, "doc_genuineness", "doc_genuineness.jsonl")
	progressPath := outputPath + ".progress.json"

	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["doc_genuineness_status"] = "running"
	version.Metadata["doc_genuineness_uri"] = outputPath
	version.Metadata["doc_genuineness_ref"] = outputPath
	version.Metadata["doc_genuineness_progress_ref"] = progressPath
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	payload := map[string]any{
		"dataset_version_id":  version.DatasetVersionID,
		"clean_artifact_ref":  cleanRef,
		"output_path":         outputPath,
		"progress_path":       progressPath,
		"doc_genuineness":     docGenConfig,
	}
	if input.DocGenuinenessPromptVer != nil && strings.TrimSpace(*input.DocGenuinenessPromptVer) != "" {
		payload["doc_genuineness_prompt_version"] = strings.TrimSpace(*input.DocGenuinenessPromptVer)
	}

	response, err := s.runWorkerTask(context.Background(), registry.TaskPathFor("dataset_doc_genuineness"), payload)
	if err != nil {
		version.Metadata["doc_genuineness_status"] = "failed"
		version.Metadata["doc_genuineness_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	now := time.Now().UTC()
	artifact := response.Artifact
	if artifact == nil && len(response.Artifacts) > 0 {
		artifact = response.Artifacts[0]
	}
	genuinenessRef := artifactString(artifact, "doc_genuineness_ref")
	if genuinenessRef == "" {
		genuinenessRef = artifactString(artifact, "doc_genuineness_uri")
	}
	if genuinenessRef == "" {
		genuinenessRef = outputPath
	}
	version.Metadata = mergeStringAny(version.Metadata, map[string]any{
		"doc_genuineness_status":       "ready",
		"doc_genuineness_uri":          genuinenessRef,
		"doc_genuineness_ref":          genuinenessRef,
		"doc_genuineness_completed_at": now,
		"doc_genuineness_notes":        response.Notes,
	})
	delete(version.Metadata, "doc_genuineness_error")
	if summary, ok := artifact["summary"].(map[string]any); ok {
		version.Metadata["doc_genuineness_summary"] = summary
		// silverone 2026-05-22 (PR-α2) — Python worker가 summary.applied 안에
		// 실행 당시 subject variables snapshot을 남긴다. 별도 top-level key로
		// 도 보존해 version metadata에서 바로 접근 가능하게.
		if applied, ok := summary["applied"].(map[string]any); ok {
			version.Metadata["doc_genuineness_applied"] = applied
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
