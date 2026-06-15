package service

import (
	"encoding/json"
	"os"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

// 전처리 빌드(doc_genuineness/clause_label) 모델 선택 allowlist 로더 (2026-06-12).
// config/lloa_models.json을 읽는다 — dataset_profiles.json과 같은 config/*.json
// 패턴. 파일이 없으면 빈 목록(모델 select 미노출, env LLOA_MODEL default만 사용).

type lloaModelCatalog struct {
	// Default — UI 기본 선택 model_id. models의 한 항목과 일치해야 한다.
	// 비어 있거나 미일치면 첫 항목이 default가 된다.
	Default string `json:"default"`
	Models  []struct {
		ModelID string `json:"model_id"`
		Label   string `json:"label"`
	} `json:"models"`
}

// loadLLOAModelOptions — catalog 파일을 읽어 LLOAModelOption 목록으로 변환한다.
// 파일 부재는 정상(빈 목록). 손상된 JSON은 error로 올려 부팅 시 fail-loud.
func loadLLOAModelOptions(path string) ([]domain.LLOAModelOption, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nil, nil
	}
	content, err := os.ReadFile(trimmed)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var catalog lloaModelCatalog
	if err := json.Unmarshal(content, &catalog); err != nil {
		return nil, err
	}

	options := make([]domain.LLOAModelOption, 0, len(catalog.Models))
	seen := map[string]struct{}{}
	for _, m := range catalog.Models {
		id := strings.TrimSpace(m.ModelID)
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		label := strings.TrimSpace(m.Label)
		if label == "" {
			label = id
		}
		options = append(options, domain.LLOAModelOption{ModelID: id, Label: label})
	}
	if len(options) == 0 {
		return nil, nil
	}

	// catalog.default 일치 항목을 default로, 없으면 첫 항목.
	defaultModel := strings.TrimSpace(catalog.Default)
	defaultIdx := 0
	for i, opt := range options {
		if opt.ModelID == defaultModel {
			defaultIdx = i
			break
		}
	}
	options[defaultIdx].Default = true
	return options, nil
}
