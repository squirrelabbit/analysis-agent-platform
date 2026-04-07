package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"
)

type StartupReconciliationSummary struct {
	BuildJobsRequeued        int `json:"build_jobs_requeued"`
	ExecutionsReenqueued     int `json:"executions_reenqueued"`
	WaitingExecutionsResumed int `json:"waiting_executions_resumed"`
}

func (s *DatasetService) ReconcileStartupBuildJobs() (int, error) {
	projects, err := s.store.ListProjects()
	if err != nil {
		return 0, err
	}

	requeued := 0
	for _, project := range projects {
		jobs, err := s.store.ListDatasetBuildJobs(project.ProjectID, "")
		if err != nil {
			return requeued, err
		}
		for _, job := range jobs {
			if job.Status != "queued" && job.Status != "running" {
				continue
			}
			if err := s.requeueDatasetBuildJob(job); err != nil {
				return requeued, err
			}
			requeued++
		}
	}
	return requeued, nil
}

func (s *DatasetService) requeueDatasetBuildJob(job domain.DatasetBuildJob) error {
	runner, err := s.recoveryDatasetBuildRunner(job)
	if err != nil {
		return err
	}

	job.ErrorMessage = nil
	job.LastErrorType = nil
	job.WorkflowRunID = nil
	if err := s.store.SaveDatasetBuildJob(job); err != nil {
		return err
	}

	if s.buildJobStarter != nil && s.buildJobStarter.EngineName() == "temporal" {
		workflowID, err := s.buildJobStarter.StartDatasetBuildWorkflow(workflows.StartDatasetBuildInput{
			JobID:            job.JobID,
			ProjectID:        job.ProjectID,
			DatasetID:        job.DatasetID,
			DatasetVersionID: job.DatasetVersionID,
			BuildType:        job.BuildType,
		})
		if err != nil {
			return err
		}
		if strings.TrimSpace(workflowID) != "" {
			job.WorkflowID = &workflowID
		}
		return s.store.SaveDatasetBuildJob(job)
	}

	go s.runDatasetBuildJob(job, runner)
	return nil
}

func (s *DatasetService) recoveryDatasetBuildRunner(job domain.DatasetBuildJob) (func() error, error) {
	switch job.BuildType {
	case datasetBuildTypePrepare:
		request, err := decodeStartupBuildRequest[domain.DatasetPrepareRequest](job.Request)
		if err != nil {
			return nil, err
		}
		return func() error {
			_, err := s.BuildPrepare(job.ProjectID, job.DatasetID, job.DatasetVersionID, request)
			return err
		}, nil
	case datasetBuildTypeSentiment:
		request, err := decodeStartupBuildRequest[domain.DatasetSentimentBuildRequest](job.Request)
		if err != nil {
			return nil, err
		}
		return func() error {
			_, err := s.BuildSentiment(job.ProjectID, job.DatasetID, job.DatasetVersionID, request)
			return err
		}, nil
	case datasetBuildTypeEmbedding:
		request, err := decodeStartupBuildRequest[domain.DatasetEmbeddingBuildRequest](job.Request)
		if err != nil {
			return nil, err
		}
		return func() error {
			_, err := s.BuildEmbeddings(job.ProjectID, job.DatasetID, job.DatasetVersionID, request)
			return err
		}, nil
	case datasetBuildTypeCluster:
		request, err := decodeStartupBuildRequest[domain.DatasetClusterBuildRequest](job.Request)
		if err != nil {
			return nil, err
		}
		return func() error {
			_, err := s.BuildClusters(job.ProjectID, job.DatasetID, job.DatasetVersionID, request)
			return err
		}, nil
	default:
		return nil, fmt.Errorf("unsupported dataset build type for reconciliation: %s", job.BuildType)
	}
}

func (s *AnalysisService) ReconcileStartupExecutions() (StartupReconciliationSummary, error) {
	summary := StartupReconciliationSummary{}
	if s.starter == nil || strings.TrimSpace(s.starter.EngineName()) == "" || s.starter.EngineName() == "noop" {
		return summary, nil
	}

	projects, err := s.store.ListProjects()
	if err != nil {
		return summary, err
	}

	for _, project := range projects {
		executions, err := s.store.ListExecutions(project.ProjectID)
		if err != nil {
			return summary, err
		}

		waitingVersions := map[string]struct{}{}
		for _, item := range executions {
			switch item.Status {
			case "queued", "running":
				execution, err := s.GetExecution(project.ProjectID, item.ExecutionID)
				if err != nil {
					return summary, err
				}
				if _, err := s.requeueExecutionInternal(
					execution,
					"control-plane restarted while execution was in-flight",
					"startup_reconciliation",
					"STARTUP_REENQUEUED",
					"execution re-enqueued during startup reconciliation",
				); err != nil {
					return summary, err
				}
				summary.ExecutionsReenqueued++
			case "waiting":
				if item.DatasetVersionID == nil {
					continue
				}
				versionID := strings.TrimSpace(*item.DatasetVersionID)
				if versionID != "" {
					waitingVersions[versionID] = struct{}{}
				}
			}
		}

		for versionID := range waitingVersions {
			resumed, err := s.ResumeWaitingExecutionsForDatasetVersion(
				project.ProjectID,
				versionID,
				"control-plane restarted and re-evaluated waiting execution dependencies",
				"startup_reconciliation",
			)
			if err != nil {
				return summary, err
			}
			summary.WaitingExecutionsResumed += resumed
		}
	}

	return summary, nil
}

func (s *AnalysisService) requeueExecutionInternal(
	execution domain.ExecutionSummary,
	reason string,
	triggeredBy string,
	eventType string,
	message string,
) (domain.ExecutionSummary, error) {
	if execution.DatasetVersionID != nil && strings.TrimSpace(*execution.DatasetVersionID) != "" {
		version, err := s.store.GetDatasetVersion(execution.ProjectID, strings.TrimSpace(*execution.DatasetVersionID))
		if err != nil && err != store.ErrNotFound {
			return domain.ExecutionSummary{}, err
		}
		if err == nil {
			execution.Plan = refreshPlanWithDatasetVersion(execution.Plan, version)
		}
	}

	workflowID, err := s.starter.StartAnalysisWorkflow(workflows.StartAnalysisInput{
		ExecutionID:      execution.ExecutionID,
		ProjectID:        execution.ProjectID,
		RequestID:        execution.RequestID,
		PlanID:           execution.Plan.PlanID,
		DatasetVersionID: execution.DatasetVersionID,
	})
	if err != nil {
		return domain.ExecutionSummary{}, err
	}

	now := time.Now().UTC()
	execution.Status = "queued"
	execution.EndedAt = nil
	execution.Events = append(execution.Events, domain.ExecutionEvent{
		ExecutionID: execution.ExecutionID,
		TS:          now,
		Level:       "info",
		EventType:   eventType,
		Message:     message,
		Payload: map[string]any{
			"reason":          reason,
			"triggered_by":    triggeredBy,
			"workflow_id":     workflowID,
			"workflow_engine": s.starter.EngineName(),
		},
	})
	if err := s.store.SaveExecution(execution); err != nil {
		return domain.ExecutionSummary{}, err
	}
	return execution, nil
}

func decodeStartupBuildRequest[T any](payload map[string]any) (T, error) {
	var result T
	raw, err := json.Marshal(payload)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return result, err
	}
	return result, nil
}
