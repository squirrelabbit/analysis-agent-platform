package service

import (
	"context"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/registry"
)

// BuildClauseLabel — ADR-017 / 5/19 결정 5-step pipeline STEP 3 (재설계).
// cleaned doc 단위로 LLOA 한 호출에 절 분리 + sentiment + aspect까지 통합
// 라벨링. PR-3로 segment 단계가 제거되고 입력 source가 segment_artifact_ref
// → clean_artifact_ref로 바뀌었다. is_relevant / scope / source_sentence_id
// 모두 schema에서 빠지고 sentiment_reason / aspect_reason 추가.
//
// Optional ``include_genuineness=["genuine_review","uncertain"]`` 명시 시
// doc_genuineness artifact를 필터링용으로 함께 inject.
//
// silverone 2026-05-28 — doc_genuineness PR-α2 패턴을 이식해 prompt에 subject
// 변수를 inject한다. metadata source는 ``dataset.metadata.doc_genuineness``를
// doc_genuineness skill과 공유한다. metadata가 없는 옛 dataset에서는 Python
// 측에서 festival default로 fallback (fail-loud 아님).
func (s *DatasetService) BuildClauseLabel(projectID, datasetID, datasetVersionID string, input domain.DatasetClauseLabelBuildRequest) (domain.DatasetVersion, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	cleanRef := cleanArtifactRef(version)
	if cleanRef == "" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "clean artifact ref missing — clause_label requires clean ready"}
	}

	// silverone 2026-06-16 (ADR-028) — verify면 문장앵커 교차모델 경로. 단일 모델
	// 경로와 별도 artifact(clause_label.verify.jsonl) + clause_label_mode="verify".
	isVerify := input.Verify != nil && *input.Verify
	outputFallback := "clause_label.jsonl"
	if isVerify {
		outputFallback = "clause_label.verify.jsonl"
	}
	outputPath := s.datasetArtifactPathOrFallback(version, "clause_label", outputFallback)
	progressPath := outputPath + ".progress.json"

	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["clause_label_status"] = "running"
	delete(version.Metadata, "clause_label_cancelled") // 재실행은 처음부터 — 이전 중단 표시 제거
	version.Metadata["clause_label_mode"] = clauseLabelMode(isVerify)
	version.Metadata["clause_label_uri"] = outputPath
	version.Metadata["clause_label_ref"] = outputPath
	version.Metadata["clause_label_progress_ref"] = progressPath
	version.Metadata["clause_label_input_source"] = "clean"
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	payload := map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"clean_artifact_ref": cleanRef,
		"output_path":        outputPath,
		"progress_path":      progressPath,
	}
	if input.ClauseLabelPromptVer != nil && strings.TrimSpace(*input.ClauseLabelPromptVer) != "" {
		payload["clause_label_prompt_version"] = strings.TrimSpace(*input.ClauseLabelPromptVer)
	}
	// silverone 2026-06-12 — 전처리 모델 선택. allowlist 검증은 job 생성 시
	// 완료(validateLLOAModelID). 생략 시 worker env(LLOA_MODEL) default.
	// silverone 2026-06-16 (ADR-028) — verify면 classify_models 2개 + judge_model을
	// 넘기고 단일 model_id는 생략(worker가 verify 경로로 위임).
	if isVerify {
		classify := make([]string, 0, len(input.ClassifyModels))
		for _, m := range input.ClassifyModels {
			if t := strings.TrimSpace(m); t != "" {
				classify = append(classify, t)
			}
		}
		payload["verify"] = true
		payload["classify_models"] = classify
		if input.JudgeModel != nil && strings.TrimSpace(*input.JudgeModel) != "" {
			payload["judge_model"] = strings.TrimSpace(*input.JudgeModel)
		}
	} else if input.ModelID != nil && strings.TrimSpace(*input.ModelID) != "" {
		payload["model_id"] = strings.TrimSpace(*input.ModelID)
	}
	// silverone 2026-05-28 — dataset.metadata.doc_genuineness를 doc_genuineness
	// skill과 공유. raw map을 그대로 pass-through하고 Python `_extract_subject_config`
	// 가 정규화한다 (recruitment_keywords는 무시 + subject_name 누락 시 festival
	// default fallback). 옛 dataset에서 metadata가 없으면 키 자체 omit.
	if rawDocGen, ok := dataset.Metadata["doc_genuineness"].(map[string]any); ok && len(rawDocGen) > 0 {
		payload["doc_genuineness"] = rawDocGen
	}
	// silverone 2026-06-17 (Phase 3) — taxonomy_id per-dataset. dataset.metadata.taxonomy_id
	// 를 payload로 넘겨 worker가 그 taxonomy로 aspect 라벨링/검증/주입한다(subject_name과
	// 동일 레일). 미설정이면 worker DEFAULT(taxonomies.DEFAULT_TAXONOMY_ID)로 fallback.
	if tid, ok := dataset.Metadata["taxonomy_id"].(string); ok && strings.TrimSpace(tid) != "" {
		payload["taxonomy_id"] = strings.TrimSpace(tid)
	}

	// 5/20 결정 — default ON. caller가 ``IncludeGenuineness``를 명시 안 하면
	// ``["genuine_review", "uncertain"]``로 자동 필터링 (non_review skip → LLOA
	// 호출 ~60% 절감 + 분석 가치 0). caller가 explicit empty list 보내면 모든
	// doc 처리 (opt-out). 어느 경로든 doc_genuineness ready 필수.
	// silverone 2026-06-16 — legacy mixed tier 제거, uncertain으로 통합.
	tiers := input.IncludeGenuineness
	optOut := input.IncludeGenuineness != nil && len(input.IncludeGenuineness) == 0
	if input.IncludeGenuineness == nil {
		tiers = []string{"genuine_review", "uncertain"}
	}
	if !optOut {
		genRef := strings.TrimSpace(metadataString(version.Metadata, "doc_genuineness_ref", ""))
		if genRef == "" {
			genRef = strings.TrimSpace(metadataString(version.Metadata, "doc_genuineness_uri", ""))
		}
		if genRef == "" {
			version.Metadata["clause_label_status"] = "failed"
			version.Metadata["clause_label_error"] = "doc_genuineness artifact not ready — clause_label default filters genuine_review+uncertain (5/20)"
			_ = s.store.SaveDatasetVersion(version)
			return domain.DatasetVersion{}, ErrInvalidArgument{Message: "doc_genuineness artifact not ready — clause_label default filters genuine_review+uncertain (5/20). Run /doc_genuineness first, or POST with include_genuineness=[] to process all docs."}
		}
		payload["include_genuineness"] = tiers
		payload["doc_genuineness_ref"] = genRef
		// ADR-026 — 사람 보정(override)을 다운스트림 필터 최상위로. worker가
		// override > final_label > genuineness 우선순위로 effective tier를 정한다.
		// (artifact 파일엔 override가 없으므로 control-plane이 주입한다.)
		if overrides, err := s.store.ListDocGenuinenessOverrides(projectID, version.DatasetVersionID); err == nil && len(overrides) > 0 {
			ov := make(map[string]string, len(overrides))
			for _, o := range overrides {
				if t := strings.TrimSpace(o.OverrideGenuineness); t != "" {
					ov[o.DocID] = t
				}
			}
			if len(ov) > 0 {
				payload["genuineness_overrides"] = ov
			}
		}
	}

	response, err := s.runWorkerTask(context.Background(), registry.TaskPathFor("dataset_clause_label"), payload)
	if err != nil {
		version.Metadata["clause_label_status"] = "failed"
		version.Metadata["clause_label_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	now := time.Now().UTC()
	artifact := response.Artifact
	if artifact == nil && len(response.Artifacts) > 0 {
		artifact = response.Artifacts[0]
	}
	clauseRef := artifactString(artifact, "clause_label_ref")
	if clauseRef == "" {
		clauseRef = artifactString(artifact, "clause_label_uri")
	}
	if clauseRef == "" {
		clauseRef = outputPath
	}
	version.Metadata = mergeStringAny(version.Metadata, map[string]any{
		"clause_label_status":         "ready",
		"clause_label_uri":            clauseRef,
		"clause_label_ref":            clauseRef,
		"clause_label_input_source":   "clean",
		"clause_label_completed_at":   now,
		"clause_label_notes":          response.Notes,
		"clause_label_prompt_version": artifactString(artifact, "prompt_version"),
	})
	delete(version.Metadata, "clause_label_error")
	if summary, ok := artifact["summary"].(map[string]any); ok {
		version.Metadata["clause_label_summary"] = summary
		// 빌드 중단(silverone 2026-06-29) — 중단 시 부분 결과는 남기지 않는다(저장/표시 X).
		// status=cancelled + ref 제거 → 뷰는 결과 미표시, 보고서/analyze는 not-ready로
		// 제외, 재실행은 처음부터(기존 재실행 버튼이 덮어씀).
		if c, _ := summary["cancelled"].(bool); c {
			version.Metadata["clause_label_cancelled"] = true
			version.Metadata["clause_label_status"] = "cancelled"
			delete(version.Metadata, "clause_label_ref")
			delete(version.Metadata, "clause_label_uri")
			// 부분 파일 삭제 — 재실행이 같은 경로로 ref를 다시 잡아도 stale이 안 보이게.
			removeArtifactFileQuietly(clauseRef)
		} else {
			delete(version.Metadata, "clause_label_cancelled")
		}
		// silverone 2026-05-28 — doc_genuineness PR-α2 패턴. Python worker가
		// summary.applied에 실행 당시 subject variables snapshot을 남긴다.
		// 별도 top-level key로도 보존해 version metadata에서 바로 접근 가능하게.
		if applied, ok := summary["applied"].(map[string]any); ok {
			version.Metadata["clause_label_applied"] = applied
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

// clauseLabelMode — verify 플래그를 metadata clause_label_mode 값으로 (ADR-028).
func clauseLabelMode(verify bool) string {
	if verify {
		return "verify"
	}
	return "single"
}
