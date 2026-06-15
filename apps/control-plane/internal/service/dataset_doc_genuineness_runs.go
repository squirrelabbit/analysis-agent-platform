package service

import (
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

// doc_genuineness 모델별 결과 보관 (silverone 2026-06-15). 같은 버전을 다른
// 모델로 재실행해도 덮어쓰지 않고 version.Metadata["doc_genuineness_runs"]에
// 모델별로 누적한다. 비교 화면이 이 목록을 읽어 한 버전 안의 두 모델을 고른다.

const docGenuinenessRunsMetaKey = "doc_genuineness_runs"

// effectiveLLOAModel — 빌드에 실제로 쓰이는 모델 id. 명시 선택(input.ModelID)이
// 있으면 그 값, 없으면 control-plane이 아는 env default(LLOA_MODEL), 둘 다 없으면
// "default". worker도 같은 env를 보므로 일치한다.
func (s *DatasetService) effectiveLLOAModel(modelID *string) string {
	if modelID != nil {
		if m := strings.TrimSpace(*modelID); m != "" {
			return m
		}
	}
	if m := strings.TrimSpace(s.lloaModel); m != "" {
		return m
	}
	return "default"
}

// docGenuinenessModelSlug — 모델 id를 파일명에 쓸 수 있게 정규화. 영숫자/-/_/.만
// 남기고 나머지(특히 '/')는 '_'로. 빈 값은 "default".
func docGenuinenessModelSlug(model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return "default"
	}
	var b strings.Builder
	for _, r := range model {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// upsertDocGenuinenessRun — runs 목록에 run을 추가하거나(같은 모델이면) 갱신한다.
// metadata에는 JSON 왕복(Postgres JSONB) 호환을 위해 []map[string]any로 저장한다.
func upsertDocGenuinenessRun(version *domain.DatasetVersion, run domain.DocGenuinenessRun) {
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	runs := docGenuinenessRunsFromMetadata(version.Metadata)
	replaced := false
	for i := range runs {
		if runs[i].Model == run.Model {
			runs[i] = run
			replaced = true
			break
		}
	}
	if !replaced {
		runs = append(runs, run)
	}
	stored := make([]map[string]any, 0, len(runs))
	for _, r := range runs {
		stored = append(stored, map[string]any{
			"model":          r.Model,
			"ref":            r.Ref,
			"prompt_version": r.PromptVersion,
			"completed_at":   r.CompletedAt.UTC().Format(time.RFC3339Nano),
		})
	}
	version.Metadata[docGenuinenessRunsMetaKey] = stored
}

// docGenuinenessRunsFromMetadata — metadata에서 runs를 파싱한다. runs 키가 없는
// 옛 버전은 단일 doc_genuineness_ref + summary.model로 run 1건을 합성한다(하위
// 호환 — 이 기능 이전에 빌드된 버전도 비교 목록에 1개로 노출).
func docGenuinenessRunsFromMetadata(metadata map[string]any) []domain.DocGenuinenessRun {
	raw, ok := metadata[docGenuinenessRunsMetaKey].([]any)
	if !ok {
		// []map[string]any로 들어온 경우(같은 프로세스 내 memory store)도 처리.
		if typed, ok2 := metadata[docGenuinenessRunsMetaKey].([]map[string]any); ok2 {
			raw = make([]any, len(typed))
			for i, m := range typed {
				raw[i] = m
			}
		}
	}
	var runs []domain.DocGenuinenessRun
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		model := strings.TrimSpace(anyStringValue(m["model"]))
		ref := strings.TrimSpace(anyStringValue(m["ref"]))
		if model == "" || ref == "" {
			continue
		}
		runs = append(runs, domain.DocGenuinenessRun{
			Model:         model,
			Ref:           ref,
			PromptVersion: strings.TrimSpace(anyStringValue(m["prompt_version"])),
			CompletedAt:   anyTimeValue(m["completed_at"]),
		})
	}
	if len(runs) > 0 {
		return runs
	}
	// 하위 호환 — 옛 단일 결과를 run 1건으로.
	ref := strings.TrimSpace(metadataString(metadata, "doc_genuineness_ref", ""))
	if ref == "" {
		ref = strings.TrimSpace(metadataString(metadata, "doc_genuineness_uri", ""))
	}
	if ref == "" {
		return nil
	}
	model := summaryMetadataString(metadata, "doc_genuineness_summary", "model")
	if model == "" {
		model = "default"
	}
	return []domain.DocGenuinenessRun{{
		Model:         model,
		Ref:           ref,
		PromptVersion: strings.TrimSpace(metadataString(metadata, "doc_genuineness_prompt_version", "")),
		CompletedAt:   anyTimeValue(metadata["doc_genuineness_completed_at"]),
	}}
}

// pendingLegacyDocGenuinenessRun — runs 키가 아직 없는 버전에서, 덮어쓰기 전의
// 단일 결과(doc_genuineness_ref + summary.model)를 run 1건으로 만들어 반환한다.
// runs 도입 이전에 빌드된 첫 결과가 새 빌드로 사라지지 않게 하는 마이그레이션용.
// 새로 쓸 파일(newOutputPath)과 같은 경로면(같은 모델 재빌드) 편입하지 않는다.
func pendingLegacyDocGenuinenessRun(metadata map[string]any, newOutputPath string) (domain.DocGenuinenessRun, bool) {
	if _, exists := metadata[docGenuinenessRunsMetaKey]; exists {
		return domain.DocGenuinenessRun{}, false // 이미 runs로 관리됨
	}
	ref := strings.TrimSpace(metadataString(metadata, "doc_genuineness_ref", ""))
	if ref == "" {
		ref = strings.TrimSpace(metadataString(metadata, "doc_genuineness_uri", ""))
	}
	if ref == "" || ref == strings.TrimSpace(newOutputPath) {
		return domain.DocGenuinenessRun{}, false
	}
	model := summaryMetadataString(metadata, "doc_genuineness_summary", "model")
	if model == "" {
		model = "default"
	}
	return domain.DocGenuinenessRun{
		Model:         model,
		Ref:           ref,
		PromptVersion: strings.TrimSpace(metadataString(metadata, "doc_genuineness_prompt_version", "")),
		CompletedAt:   anyTimeValue(metadata["doc_genuineness_completed_at"]),
	}, true
}

// findDocGenuinenessRun — 모델 id로 run 1건 조회.
func findDocGenuinenessRun(runs []domain.DocGenuinenessRun, model string) (domain.DocGenuinenessRun, bool) {
	model = strings.TrimSpace(model)
	for _, r := range runs {
		if r.Model == model {
			return r, true
		}
	}
	return domain.DocGenuinenessRun{}, false
}

// GetDocGenuinenessRuns — 한 버전에 보관된 모델별 결과 목록(비교 화면 dropdown용).
// model_display_name은 응답 시점에 env 기반으로 채운다.
func (s *DatasetService) GetDocGenuinenessRuns(projectID, datasetID, datasetVersionID string) (domain.DocGenuinenessRunsResponse, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DocGenuinenessRunsResponse{}, err
	}
	runs := docGenuinenessRunsFromMetadata(version.Metadata)
	for i := range runs {
		runs[i].ModelDisplayName = s.modelDisplayNameFor(runs[i].Model)
	}
	if runs == nil {
		runs = []domain.DocGenuinenessRun{}
	}
	return domain.DocGenuinenessRunsResponse{
		DatasetVersionID: datasetVersionID,
		Items:            runs,
	}, nil
}

// anyTimeValue — metadata 값에서 time.Time 회수. time.Time(memory store) 또는
// RFC3339 문자열(JSONB 왕복) 모두 처리. 실패 시 zero time.
func anyTimeValue(v any) time.Time {
	switch t := v.(type) {
	case time.Time:
		return t
	case string:
		if parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(t)); err == nil {
			return parsed
		}
	}
	return time.Time{}
}
