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
// (genuine_review / non_review / uncertain) 라벨을 부여한다. clean ready
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

	// verify 모드(ADR-026): 모델 2개 교차 분류 + 불일치 judge. 단일 모델 경로와
	// artifact 스키마가 달라 별도 흐름으로 분기한다(per-model runs 미사용).
	if input.Verify != nil && *input.Verify {
		return s.buildDocGenuinenessVerify(version, docGenConfig, cleanRef, input)
	}

	// silverone 2026-06-15 — 모델별 결과를 덮어쓰지 않고 보관하려고 출력 파일을
	// 모델 slug로 분리한다. 같은 모델 재실행은 같은 파일을 덮고(=그 모델 최신),
	// 다른 모델은 별도 파일로 남아 비교에 쓰인다. doc_genuineness_ref(단일 view +
	// 후속 clause_label 입력)는 이번 실행 파일을 가리켜 "최신" 의미를 유지한다.
	effectiveModel := s.effectiveLLOAModel(input.ModelID)
	outputPath := s.datasetArtifactPathOrFallback(version, "doc_genuineness", "doc_genuineness."+docGenuinenessModelSlug(effectiveModel)+".jsonl")
	progressPath := outputPath + ".progress.json"

	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	// 마이그레이션 빈틈 메움 — runs 도입 *이전*에 빌드된 단일 결과는 runs에
	// 없다. 아래에서 doc_genuineness_ref/summary를 이번 실행 값으로 덮어쓰기
	// 전에, runs 키가 없으면 기존 단일 결과를 run 1건으로 미리 편입한다(다른
	// 모델·다른 파일일 때만). 그래야 새 모델 1회 실행만으로 옛 결과와 비교 가능.
	legacyRun, hasLegacy := pendingLegacyDocGenuinenessRun(version.Metadata, outputPath)
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
	// silverone 2026-06-12 — 전처리 모델 선택. allowlist 검증은 job 생성 시
	// 완료(validateLLOAModelID). 생략 시 worker env(LLOA_MODEL) default.
	if input.ModelID != nil && strings.TrimSpace(*input.ModelID) != "" {
		payload["model_id"] = strings.TrimSpace(*input.ModelID)
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
	promptVersion := ""
	if summary, ok := artifact["summary"].(map[string]any); ok {
		version.Metadata["doc_genuineness_summary"] = summary
		// silverone 2026-05-22 (PR-α2) — Python worker가 summary.applied 안에
		// 실행 당시 subject variables snapshot을 남긴다. 별도 top-level key로
		// 도 보존해 version metadata에서 바로 접근 가능하게.
		if applied, ok := summary["applied"].(map[string]any); ok {
			version.Metadata["doc_genuineness_applied"] = applied
		}
		promptVersion = anyStringValue(summary["prompt_version"])
	}
	// silverone 2026-06-15 — 모델별 결과 누적. 같은 모델은 갱신, 다른 모델은 추가.
	// 비교 화면(GetDocGenuinenessRuns/CompareDocGenuineness)이 이 목록을 읽는다.
	// 먼저 옛 단일 결과(있으면)를 편입한 뒤 이번 실행을 올린다.
	if hasLegacy {
		upsertDocGenuinenessRun(&version, legacyRun)
	}
	upsertDocGenuinenessRun(&version, domain.DocGenuinenessRun{
		Model:         effectiveModel,
		Ref:           genuinenessRef,
		PromptVersion: promptVersion,
		CompletedAt:   now,
	})
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	if err := s.attachDatasetVersionArtifacts(&version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
}

// buildDocGenuinenessVerify — ADR-026 verify 모드. 모델 2개 교차 분류 + 불일치
// judge로 final_label 산출. 단일 모델 경로와 별도 artifact(doc_genuineness.verify
// .jsonl) + doc_genuineness_mode="verify". per-model runs registry는 쓰지 않는다.
func (s *DatasetService) buildDocGenuinenessVerify(
	version domain.DatasetVersion, docGenConfig map[string]any, cleanRef string,
	input domain.DatasetDocGenuinenessBuildRequest,
) (domain.DatasetVersion, error) {
	outputPath := s.datasetArtifactPathOrFallback(version, "doc_genuineness", "doc_genuineness.verify.jsonl")
	progressPath := outputPath + ".progress.json"

	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["doc_genuineness_status"] = "running"
	version.Metadata["doc_genuineness_mode"] = "verify"
	version.Metadata["doc_genuineness_uri"] = outputPath
	version.Metadata["doc_genuineness_ref"] = outputPath
	version.Metadata["doc_genuineness_progress_ref"] = progressPath
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	classify := make([]string, 0, len(input.ClassifyModels))
	for _, m := range input.ClassifyModels {
		if t := strings.TrimSpace(m); t != "" {
			classify = append(classify, t)
		}
	}
	payload := map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"clean_artifact_ref": cleanRef,
		"output_path":        outputPath,
		"progress_path":      progressPath,
		"doc_genuineness":    docGenConfig,
		"verify":             true,
		"classify_models":    classify,
	}
	if input.DocGenuinenessPromptVer != nil && strings.TrimSpace(*input.DocGenuinenessPromptVer) != "" {
		payload["doc_genuineness_prompt_version"] = strings.TrimSpace(*input.DocGenuinenessPromptVer)
	}
	if input.JudgeModel != nil && strings.TrimSpace(*input.JudgeModel) != "" {
		payload["judge_model"] = strings.TrimSpace(*input.JudgeModel)
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
		genuinenessRef = outputPath
	}
	version.Metadata = mergeStringAny(version.Metadata, map[string]any{
		"doc_genuineness_status":       "ready",
		"doc_genuineness_mode":         "verify",
		"doc_genuineness_uri":          genuinenessRef,
		"doc_genuineness_ref":          genuinenessRef,
		"doc_genuineness_completed_at": now,
		"doc_genuineness_notes":        response.Notes,
	})
	delete(version.Metadata, "doc_genuineness_error")
	// verify 결과는 per-model runs와 무관하므로 runs 레지스트리는 건드리지 않는다.
	delete(version.Metadata, "doc_genuineness_runs")
	if summary, ok := artifact["summary"].(map[string]any); ok {
		version.Metadata["doc_genuineness_summary"] = summary
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
