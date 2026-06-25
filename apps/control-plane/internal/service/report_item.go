package service

import (
	"encoding/json"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"
)

// AppendReportItem — 기존 보고서에 item(블록) 1개를 추가한다. 새 보고서를 만들지 않고
// report_id의 blocks 뒤에 append + updated_at만 갱신한다(채팅 결과 → 보고서 추가 flow).
//
// type=analysis_result는 run_id가 가리키는 완료된 분석 결과를 그 시점 스냅샷으로 박제한다
// (보관함 CreateSavedResult와 동일한 추출). 스냅샷 이유: 재실행/재빌드로 결과가 달라지거나
// thread를 지워도 보고서 근거는 저장 당시 그대로 남아야 감사·재현이 된다.
func (s *DatasetService) AppendReportItem(projectID, reportID string, req domain.ReportItemAppendRequest) (domain.ReportItemAppendResponse, error) {
	report, err := s.store.GetReport(projectID, strings.TrimSpace(reportID))
	if err != nil {
		if err == store.ErrNotFound {
			return domain.ReportItemAppendResponse{}, ErrNotFound{Resource: "report"}
		}
		return domain.ReportItemAppendResponse{}, err
	}

	itemType := strings.TrimSpace(req.Type)
	if itemType == "" {
		itemType = "analysis_result"
	}

	item := map[string]any{
		"uid":  id.New(),
		"type": itemType,
	}
	if strings.TrimSpace(req.Interp) != "" {
		item["interp"] = req.Interp
	}
	if req.Options != nil {
		item["options"] = req.Options
	}
	if req.Layout != nil {
		item["layout"] = req.Layout
	}

	switch itemType {
	case "analysis_result":
		built, err := s.buildAnalysisResultItem(projectID, req)
		if err != nil {
			return domain.ReportItemAppendResponse{}, err
		}
		for k, v := range built {
			item[k] = v
		}
	default:
		// text 등 비-분석 블록 — title만(향후 type별 확장 지점).
		item["title"] = strings.TrimSpace(req.Title)
	}

	// 기존 blocks를 읽어 뒤에 append(전체 교체 store라 read-modify-write).
	blocks := []any{}
	if len(report.Blocks) > 0 {
		if err := json.Unmarshal(report.Blocks, &blocks); err != nil {
			return domain.ReportItemAppendResponse{}, ErrInvalidArgument{Message: "report blocks is not a JSON array"}
		}
	}
	blocks = append(blocks, item)
	encoded, err := json.Marshal(blocks)
	if err != nil {
		return domain.ReportItemAppendResponse{}, err
	}
	report.Blocks = encoded
	report.UpdatedAt = time.Now().UTC()
	if err := s.store.UpdateReport(report); err != nil {
		return domain.ReportItemAppendResponse{}, err
	}

	saved, err := s.store.GetReport(projectID, report.ReportID)
	if err != nil {
		return domain.ReportItemAppendResponse{}, err
	}
	return domain.ReportItemAppendResponse{Report: saved, Item: item}, nil
}

// buildAnalysisResultItem — run_id의 완료 분석 결과에서 question/assistant_content/
// display/plan을 추출해 item 필드로 만든다(보관함 추출 로직 재사용).
func (s *DatasetService) buildAnalysisResultItem(projectID string, req domain.ReportItemAppendRequest) (map[string]any, error) {
	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		return nil, ErrInvalidArgument{Message: "run_id is required for analysis_result"}
	}
	run, err := s.store.GetAnalysisRun(projectID, runID)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, ErrNotFound{Resource: "analysis run"}
		}
		return nil, err
	}
	// GetAnalysisRun이 project_id로 필터하므로 project 일치는 보장. thread는 명시 시 검증.
	if tid := strings.TrimSpace(req.ThreadID); tid != "" && tid != run.ThreadID {
		return nil, ErrInvalidArgument{Message: "thread_id does not match the run"}
	}
	if run.Status != "completed" || len(run.ResultJSON) == 0 {
		return nil, ErrInvalidArgument{Message: "only completed analysis results can be added to a report"}
	}

	display := extractDisplayFromResultJSON(run.ResultJSON)
	plan := extractPlanFromResultJSON(run.ResultJSON)
	assistantContent := extractAssistantContentFromResultJSON(run.ResultJSON)
	question := analysisRunUserQuestion(run)

	title := strings.TrimSpace(req.Title)
	if title == "" {
		title = deriveSavedResultTitle(display, question, assistantContent)
	}
	title = truncateRunes(title, savedResultTitleMaxLen)

	return map[string]any{
		"run_id":            run.RunID,
		"thread_id":         run.ThreadID,
		"title":             title,
		"question":          question,
		"assistant_content": assistantContent,
		"display":           display,
		"plan":              plan,
	}, nil
}
