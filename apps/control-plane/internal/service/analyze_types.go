package service

import (
	"encoding/json"
	"strings"
)

// analyze 데이터 계약(요청/응답 타입) 모음. silverone 2026-06-04 — 구조 파악
// 난이도를 낮추기 위해 analyze.go(로직)에서 데이터 계약 타입만 이 파일로 분리했다.
// package 내 파일 이동일 뿐 public API/동작은 동일하다 (service_decomposition.md 3단계).

// AnalyzeRequest — 서비스 내부에서 두 모드 합쳐 처리하는 unified request.
// HTTP 진입점은 path별로 모드 1개씩 분리 (K-안, 2026-05-22).
type AnalyzeRequest struct {
	Plan                json.RawMessage  `json:"plan,omitempty"`
	UserQuestion        string           `json:"user_question,omitempty"`
	ConversationContext []map[string]any `json:"conversation_context,omitempty"`
	// silverone 2026-05-26 (ADR-020 PR-A) — reuse 분기에서 worker composer가
	// reuse_applied 템플릿을 선택할 수 있도록 hint를 전달. 외부 caller는 채울 일
	// 없음. tryReusePlan만 사용.
	ReuseMetadata map[string]any `json:"-"`
}

// AnalyzeUserQuestionRequest — 화면 분석 path(/datasets/{did}/analyze)
// request body. user_question만 받는다. plan 필드는 받지 않음.
type AnalyzeUserQuestionRequest struct {
	UserQuestion string `json:"user_question"`
}

// AnalyzeDebugRequest — version-specific path(/versions/{vid}/analyze) request
// body. plan만 받는다 (debug/replay 전용). user_question 필드는 받지 않음.
type AnalyzeDebugRequest struct {
	Plan json.RawMessage `json:"plan"`
}

// AnalyzeResponse — Python worker 응답을 그대로 passthrough + 최소 metadata.
type AnalyzeResponse struct {
	ProjectID string          `json:"project_id"`
	DatasetID string          `json:"dataset_id"`
	VersionID string          `json:"version_id"`
	Mode      string          `json:"mode"`
	Result    json.RawMessage `json:"result"`
}

// analyzeArtifactPaths — Python worker에 inject되는 path map.
// ClauseKeywords는 optional — 키워드 build이 돈 버전에만 존재. 비어 있으면 payload에서 생략.
type analyzeArtifactPaths struct {
	Docs           string
	Clauses        string
	Genuineness    string
	ClauseKeywords string
}

func (p analyzeArtifactPaths) asPayload() map[string]string {
	out := map[string]string{
		"docs":        p.Docs,
		"clauses":     p.Clauses,
		"genuineness": p.Genuineness,
	}
	// optional — 있을 때만 주입(없으면 worker가 clause_keywords view를 안 만든다).
	if strings.TrimSpace(p.ClauseKeywords) != "" {
		out["clause_keywords"] = p.ClauseKeywords
	}
	return out
}
