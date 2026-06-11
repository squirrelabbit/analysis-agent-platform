package service

import (
	"encoding/json"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"
)

// 보고서 보관함 (silverone 2026-06-10).
//
// 채팅에서 만든 분석 결과를 "보고서 저장"하면, 그 시점의 run.result_json에서
// composer.display / plan / assistant_content를 frontend-safe keep-set으로
// 추출해 report_saved_results에 스냅샷으로 박제한다. 보고서 탭은 이 보관함을
// 목록으로 보여주고, 사용자가 골라 보고서 블록으로 구성한다(블록 편집·보고서
// 문서 저장은 다음 단계).
//
// 스냅샷을 박제하는 이유: 같은 질문을 재실행하면 결과가 달라질 수 있고
// (LLM plan 비결정성·dataset 재빌드), thread를 지울 수도 있다. 보고서에 들어간
// 근거는 저장 당시 그대로 남아야 감사·재현이 가능하다.

const savedResultTitleMaxLen = 120

// CreateSavedResult — run_id가 가리키는 완료된 분석 결과를 보관함에 스냅샷한다.
// project는 path에서, dataset/version은 run에서 유도한다. completed가 아니거나
// result_json이 비어 있으면 저장하지 않는다(빈 보관함 카드 방지).
func (s *DatasetService) CreateSavedResult(projectID string, input domain.ReportSavedResultCreateRequest) (domain.ReportSavedResult, error) {
	runID := strings.TrimSpace(input.RunID)
	if runID == "" {
		return domain.ReportSavedResult{}, ErrInvalidArgument{Message: "run_id is required"}
	}
	run, err := s.store.GetAnalysisRun(projectID, runID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.ReportSavedResult{}, ErrNotFound{Resource: "analysis run"}
		}
		return domain.ReportSavedResult{}, err
	}
	// thread_id가 함께 오면 일치 검증 (잘못된 thread의 run 저장 방지).
	if tid := strings.TrimSpace(input.ThreadID); tid != "" && tid != run.ThreadID {
		return domain.ReportSavedResult{}, ErrInvalidArgument{Message: "thread_id does not match the run"}
	}
	if run.Status != "completed" || len(run.ResultJSON) == 0 {
		return domain.ReportSavedResult{}, ErrInvalidArgument{Message: "only completed analysis results can be saved to the report library"}
	}

	display := extractDisplayFromResultJSON(run.ResultJSON)
	plan := extractPlanFromResultJSON(run.ResultJSON)
	assistantContent := extractAssistantContentFromResultJSON(run.ResultJSON)
	question := analysisRunUserQuestion(run)

	title := strings.TrimSpace(input.Title)
	if title == "" {
		title = deriveSavedResultTitle(display, question, assistantContent)
	}
	title = truncateRunes(title, savedResultTitleMaxLen)

	result := domain.ReportSavedResult{
		ResultID:         id.New(),
		ProjectID:        projectID,
		DatasetID:        run.DatasetID,
		DatasetVersionID: run.DatasetVersionID,
		ThreadID:         run.ThreadID,
		RunID:            run.RunID,
		SourceMessageID:  strings.TrimSpace(run.UserMessageID),
		Title:            title,
		Question:         question,
		AssistantContent: assistantContent,
		Display:          display,
		Plan:             plan,
	}
	if err := s.store.SaveReportSavedResult(result); err != nil {
		return domain.ReportSavedResult{}, err
	}
	return s.store.GetReportSavedResult(projectID, result.ResultID)
}

// ListSavedResults — project의 보관함 목록(최신순). datasetID가 비면 전체.
func (s *DatasetService) ListSavedResults(projectID, datasetID string) (domain.ReportSavedResultListResponse, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ReportSavedResultListResponse{}, ErrNotFound{Resource: "project"}
		}
		return domain.ReportSavedResultListResponse{}, err
	}
	items, err := s.store.ListReportSavedResults(projectID, strings.TrimSpace(datasetID))
	if err != nil {
		return domain.ReportSavedResultListResponse{}, err
	}
	return domain.ReportSavedResultListResponse{Items: items}, nil
}

// DeleteSavedResult — 보관함 항목 삭제. 보고서가 이미 이 결과를 참조하더라도
// 스냅샷은 보고서 블록 내부에 복제되지 않으므로(현재는 보관함만), 삭제 시
// 해당 항목은 더 이상 새 블록에 추가할 수 없다.
func (s *DatasetService) DeleteSavedResult(projectID, resultID string) error {
	if err := s.store.DeleteReportSavedResult(projectID, strings.TrimSpace(resultID)); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "saved result"}
		}
		return err
	}
	return nil
}

// extractAssistantContentFromResultJSON — run.result_json의 composer.assistant_content.
func extractAssistantContentFromResultJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return ""
	}
	composer, ok := root["composer"].(map[string]any)
	if !ok {
		return ""
	}
	content, _ := composer["assistant_content"].(string)
	return strings.TrimSpace(content)
}

// analysisRunUserQuestion — run.request_json에서 사용자 질문 추출. analyze 요청은
// user_question 키로 질문을 싣는다.
func analysisRunUserQuestion(run domain.AnalysisRun) string {
	if run.RequestJSON == nil {
		return ""
	}
	if q, ok := run.RequestJSON["user_question"].(string); ok {
		return strings.TrimSpace(q)
	}
	return ""
}

// deriveSavedResultTitle — 제목 미지정 시 display.title → question → assistant_content
// 순으로 유도. 모두 비면 기본값.
func deriveSavedResultTitle(display map[string]any, question, assistantContent string) string {
	if display != nil {
		if t, ok := display["title"].(string); ok {
			if t = strings.TrimSpace(t); t != "" {
				return t
			}
		}
	}
	if q := strings.TrimSpace(question); q != "" {
		return q
	}
	if c := strings.TrimSpace(assistantContent); c != "" {
		return c
	}
	return "분석 결과"
}
