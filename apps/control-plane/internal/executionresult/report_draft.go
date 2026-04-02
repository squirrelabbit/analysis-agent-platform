package executionresult

import (
	"fmt"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

func BuildListItem(execution domain.ExecutionSummary) domain.ExecutionListItem {
	result := executionResultForPresentation(execution)
	item := domain.ExecutionListItem{
		ExecutionID:      execution.ExecutionID,
		Status:           execution.Status,
		CreatedAt:        execution.CreatedAt,
		EndedAt:          execution.EndedAt,
		DatasetVersionID: execution.DatasetVersionID,
	}
	if result.PrimarySkillName != nil {
		item.PrimarySkillName = stringPointer(strings.TrimSpace(*result.PrimarySkillName))
	}
	if result.Answer != nil {
		summary := strings.TrimSpace(result.Answer.Summary)
		if summary != "" {
			item.AnswerPreview = stringPointer(summary)
		}
	}
	item.WarningCount = len(result.Warnings)
	if result.Waiting != nil {
		item.Waiting = result.Waiting
	}
	return item
}

func BuildReportDraftV1(title string, executions []domain.ExecutionSummary) domain.ReportDraftV1 {
	sections := make([]domain.ReportDraftSection, 0, len(executions))
	keyFindings := make([]string, 0)
	evidence := make([]map[string]any, 0)
	followUpQuestions := make([]string, 0)
	warnings := make([]string, 0)
	usageSummary := map[string]any{}
	summaries := make([]string, 0, len(executions))

	for _, execution := range executions {
		result := executionResultForPresentation(execution)
		section := domain.ReportDraftSection{
			ExecutionID:  execution.ExecutionID,
			Status:       execution.Status,
			CreatedAt:    execution.CreatedAt,
			WarningCount: len(result.Warnings),
		}
		if result.PrimarySkillName != nil {
			section.PrimarySkillName = stringPointer(strings.TrimSpace(*result.PrimarySkillName))
		}
		if result.Answer != nil {
			section.Summary = strings.TrimSpace(result.Answer.Summary)
			section.KeyFindings = limitStrings(result.Answer.KeyFindings, 5)
			section.Evidence = limitEvidence(result.Answer.Evidence, 3)
			followUpQuestions = append(followUpQuestions, result.Answer.FollowUpQuestions...)
		}
		if section.Summary == "" {
			section.Summary = fmt.Sprintf("execution %s는 %s 상태입니다.", execution.ExecutionID, execution.Status)
		}
		summaries = append(summaries, section.Summary)
		keyFindings = append(keyFindings, section.KeyFindings...)
		evidence = append(evidence, annotateEvidenceWithExecution(execution.ExecutionID, section.Evidence)...)
		warnings = append(warnings, result.Warnings...)
		if len(result.UsageSummary) > 0 {
			usageSummary = mergeExecutionUsage(usageSummary, result.UsageSummary)
		}
		sections = append(sections, section)
	}

	draft := domain.ReportDraftV1{
		SchemaVersion:     "report-draft-v1",
		Title:             strings.TrimSpace(title),
		ExecutionCount:    len(executions),
		Sections:          sections,
		KeyFindings:       limitStrings(uniqueNonEmptyStrings(keyFindings), 12),
		Evidence:          limitEvidence(evidence, 8),
		FollowUpQuestions: limitStrings(uniqueNonEmptyStrings(followUpQuestions), 8),
		Warnings:          uniqueNonEmptyStrings(warnings),
	}
	if len(usageSummary) > 0 {
		draft.UsageSummary = usageSummary
	}
	draft.Overview = buildReportDraftOverview(draft.Title, summaries, draft.KeyFindings, len(executions))
	return draft
}

func executionResultForPresentation(execution domain.ExecutionSummary) domain.ExecutionResultV1 {
	if execution.ResultV1Snapshot != nil {
		return *execution.ResultV1Snapshot
	}
	return BuildV1(execution)
}

func buildReportDraftOverview(title string, summaries, findings []string, executionCount int) string {
	title = strings.TrimSpace(title)
	switch {
	case len(findings) > 0:
		return fmt.Sprintf("%s: 선택한 %d개 실행 결과 기준 핵심 관찰은 %s", title, executionCount, strings.Join(limitStrings(findings, 3), " "))
	case len(summaries) > 0:
		return fmt.Sprintf("%s: 선택한 %d개 실행 결과를 바탕으로 %s", title, executionCount, strings.Join(limitStrings(summaries, 2), " "))
	default:
		return fmt.Sprintf("%s: 선택한 %d개 실행 결과를 묶은 보고서 초안입니다.", title, executionCount)
	}
}

func annotateEvidenceWithExecution(executionID string, evidence []map[string]any) []map[string]any {
	items := make([]map[string]any, 0, len(evidence))
	for _, item := range evidence {
		cloned := map[string]any{
			"execution_id": executionID,
		}
		for key, value := range item {
			cloned[key] = value
		}
		items = append(items, cloned)
	}
	return items
}

func limitStrings(items []string, limit int) []string {
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}

func limitEvidence(items []map[string]any, limit int) []map[string]any {
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}
