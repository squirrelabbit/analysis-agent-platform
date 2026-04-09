package service

import (
	"sort"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

const operationsFailureLimit = 5

func (s *AnalysisService) GetOperationsSummary(projectID string) (domain.OperationsSummaryResponse, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.OperationsSummaryResponse{}, ErrNotFound{Resource: "project"}
		}
		return domain.OperationsSummaryResponse{}, err
	}

	executions, err := s.store.ListExecutions(projectID)
	if err != nil {
		return domain.OperationsSummaryResponse{}, err
	}
	jobs, err := s.store.ListDatasetBuildJobs(projectID, "")
	if err != nil {
		return domain.OperationsSummaryResponse{}, err
	}

	response := domain.OperationsSummaryResponse{
		ProjectID:   projectID,
		GeneratedAt: time.Now().UTC(),
		Executions: domain.OperationsExecutionSummary{
			ByStatus:            map[string]int{},
			WaitingByDependency: map[string]int{},
			FinalAnswerByStatus: map[string]int{},
		},
		BuildJobs: domain.OperationsBuildJobSummary{
			ByStatus: map[string]int{},
			ByType:   map[string]map[string]int{},
		},
	}

	executionFailures := make([]domain.OperationsFailureItem, 0)
	for _, execution := range executions {
		execution = withExecutionDiagnostics(execution)
		response.Executions.Total++
		status := strings.TrimSpace(execution.Status)
		if status == "" {
			status = "unknown"
		}
		response.Executions.ByStatus[status]++
		if execution.Diagnostics != nil {
			if execution.Diagnostics.Waiting != nil {
				waitingFor := strings.TrimSpace(execution.Diagnostics.Waiting.WaitingFor)
				if waitingFor == "" {
					waitingFor = "unknown"
				}
				response.Executions.WaitingByDependency[waitingFor]++
			}
			if finalAnswerStatus := strings.TrimSpace(execution.Diagnostics.FinalAnswerStatus); finalAnswerStatus != "" {
				response.Executions.FinalAnswerByStatus[finalAnswerStatus]++
			}
			if status == "failed" {
				executionFailures = append(executionFailures, domain.OperationsFailureItem{
					ID:          execution.ExecutionID,
					Status:      status,
					Type:        strings.TrimSpace(execution.Diagnostics.LatestEventType),
					Message:     firstNonEmpty(execution.Diagnostics.FailureReason, execution.Diagnostics.LatestEventMessage),
					OccurredAt:  latestExecutionTimestamp(execution),
					ResourceRef: strings.TrimSpace(optionalStringValue(execution.DatasetVersionID)),
				})
			}
		}
	}
	response.Executions.RecentFailures = sortAndTrimFailures(executionFailures, operationsFailureLimit)

	buildFailures := make([]domain.OperationsFailureItem, 0)
	for _, job := range jobs {
		job = withBuildJobDiagnostics(job)
		response.BuildJobs.Total++
		status := strings.TrimSpace(job.Status)
		if status == "" {
			status = "unknown"
		}
		response.BuildJobs.ByStatus[status]++
		buildType := strings.TrimSpace(job.BuildType)
		if buildType == "" {
			buildType = "unknown"
		}
		typeSummary := response.BuildJobs.ByType[buildType]
		if typeSummary == nil {
			typeSummary = map[string]int{}
			response.BuildJobs.ByType[buildType] = typeSummary
		}
		typeSummary[status]++
		if job.Diagnostics != nil && job.Diagnostics.RetryCount > 0 {
			response.BuildJobs.RetryingJobs++
		}
		if status == "failed" {
			buildFailures = append(buildFailures, domain.OperationsFailureItem{
				ID:          job.JobID,
				Status:      status,
				Type:        strings.TrimSpace(optionalStringValue(job.LastErrorType)),
				Message:     strings.TrimSpace(optionalStringValue(job.ErrorMessage)),
				OccurredAt:  latestBuildJobTimestamp(job),
				RetryCount:  max(job.Attempt-1, 0),
				ResourceRef: strings.TrimSpace(job.DatasetVersionID),
			})
		}
	}
	response.BuildJobs.RecentFailures = sortAndTrimFailures(buildFailures, operationsFailureLimit)

	return response, nil
}

func latestExecutionTimestamp(execution domain.ExecutionSummary) *time.Time {
	if len(execution.Events) > 0 {
		ts := execution.Events[len(execution.Events)-1].TS
		return &ts
	}
	if execution.EndedAt != nil {
		return execution.EndedAt
	}
	ts := execution.CreatedAt
	return &ts
}

func latestBuildJobTimestamp(job domain.DatasetBuildJob) *time.Time {
	if job.CompletedAt != nil {
		return job.CompletedAt
	}
	if job.StartedAt != nil {
		return job.StartedAt
	}
	ts := job.CreatedAt
	return &ts
}

func sortAndTrimFailures(items []domain.OperationsFailureItem, limit int) []domain.OperationsFailureItem {
	sort.Slice(items, func(i, j int) bool {
		left := items[i].OccurredAt
		right := items[j].OccurredAt
		if left == nil {
			return false
		}
		if right == nil {
			return true
		}
		return left.After(*right)
	})
	if len(items) > limit {
		items = items[:limit]
	}
	return items
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func max(left, right int) int {
	if left > right {
		return left
	}
	return right
}
