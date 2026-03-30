package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/planner"
	"analysis-support-platform/control-plane/internal/registry"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"
)

type AnalysisService struct {
	store   store.Repository
	starter workflows.Starter
	planner planner.Planner
}

func NewAnalysisService(repository store.Repository, starter workflows.Starter, planGenerator planner.Planner) *AnalysisService {
	return &AnalysisService{
		store:   repository,
		starter: starter,
		planner: planGenerator,
	}
}

func (s *AnalysisService) SubmitAnalysis(projectID string, input domain.AnalysisSubmitRequest) (domain.AnalysisPlanResponse, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.AnalysisPlanResponse{}, ErrNotFound{Resource: "project"}
		}
		return domain.AnalysisPlanResponse{}, err
	}

	if input.DatasetVersionID == nil || strings.TrimSpace(*input.DatasetVersionID) == "" {
		return domain.AnalysisPlanResponse{}, ErrInvalidArgument{Message: "dataset_version_id is required"}
	}
	if strings.TrimSpace(input.Goal) == "" {
		return domain.AnalysisPlanResponse{}, ErrInvalidArgument{Message: "goal is required"}
	}
	if input.Constraints == nil {
		input.Constraints = []string{}
	}
	if input.Context == nil {
		input.Context = map[string]any{}
	}
	version, err := s.store.GetDatasetVersion(projectID, strings.TrimSpace(*input.DatasetVersionID))
	if err != nil {
		if err == store.ErrNotFound {
			return domain.AnalysisPlanResponse{}, ErrNotFound{Resource: "dataset version"}
		}
		return domain.AnalysisPlanResponse{}, err
	}
	if input.DatasetName == nil || strings.TrimSpace(*input.DatasetName) == "" {
		input.DatasetName = &version.StorageURI
	}
	if input.DataType == nil || strings.TrimSpace(*input.DataType) == "" {
		input.DataType = &version.DataType
	}

	request := domain.AnalysisRequest{
		RequestID:        id.New(),
		ProjectID:        projectID,
		DatasetName:      input.DatasetName,
		DatasetVersionID: input.DatasetVersionID,
		Goal:             strings.TrimSpace(input.Goal),
		Constraints:      input.Constraints,
		Context:          input.Context,
		RequestedPlan:    input.RequestedPlan,
		CreatedAt:        time.Now().UTC(),
	}
	if err := s.store.SaveRequest(request); err != nil {
		return domain.AnalysisPlanResponse{}, err
	}

	plan := input.RequestedPlan
	plannerType := "stub"
	var plannerModel *string
	var plannerPromptVersion *string
	if plan == nil {
		if s.planner != nil {
			generated, err := s.planner.GeneratePlan(context.Background(), input)
			if err != nil {
				return domain.AnalysisPlanResponse{}, err
			}
			plan = &generated.Plan
			plannerType = strings.TrimSpace(generated.PlannerType)
			if plannerType == "" {
				plannerType = "python-ai"
			}
			plannerModel = generated.PlannerModel
			plannerPromptVersion = generated.PlannerPromptVersion
		} else {
			generated := buildDefaultPlan(input)
			plan = &generated
		}
	}

	datasetName := "dataset_from_version"
	if input.DatasetName != nil && strings.TrimSpace(*input.DatasetName) != "" {
		datasetName = strings.TrimSpace(*input.DatasetName)
	}
	normalizedPlan := normalizePlan(*plan, datasetName, version, input.Goal)
	planHash := computePlanHash(normalizedPlan)
	planRecord := domain.PlanRecord{
		PlanID:               normalizedPlan.PlanID,
		RequestID:            request.RequestID,
		ProjectID:            projectID,
		DatasetName:          datasetName,
		DatasetVersionID:     input.DatasetVersionID,
		Plan:                 normalizedPlan,
		Status:               "draft",
		PlannerType:          &plannerType,
		PlannerModel:         plannerModel,
		PlannerPromptVersion: plannerPromptVersion,
		PlanHash:             &planHash,
		CreatedAt:            time.Now().UTC(),
	}
	if err := s.store.SavePlan(planRecord); err != nil {
		return domain.AnalysisPlanResponse{}, err
	}

	return domain.AnalysisPlanResponse{
		Request: request,
		Plan:    planRecord,
	}, nil
}

func (s *AnalysisService) GetRequest(projectID, requestID string) (domain.AnalysisRequest, error) {
	request, err := s.store.GetRequest(projectID, requestID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.AnalysisRequest{}, ErrNotFound{Resource: "analysis request"}
		}
		return domain.AnalysisRequest{}, err
	}
	return request, nil
}

func (s *AnalysisService) GetPlan(projectID, planID string) (domain.PlanRecord, error) {
	plan, err := s.store.GetPlan(projectID, planID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.PlanRecord{}, ErrNotFound{Resource: "plan"}
		}
		return domain.PlanRecord{}, err
	}
	return plan, nil
}

func (s *AnalysisService) ExecutePlan(projectID, planID string) (domain.PlanExecuteResponse, error) {
	plan, err := s.store.GetPlan(projectID, planID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.PlanExecuteResponse{}, ErrNotFound{Resource: "plan"}
		}
		return domain.PlanExecuteResponse{}, err
	}

	executionID := id.New()
	jobID := id.New()
	now := time.Now().UTC()
	execution := domain.ExecutionSummary{
		ExecutionID:        executionID,
		ProjectID:          projectID,
		RequestID:          plan.RequestID,
		Plan:               plan.Plan,
		Status:             "queued",
		RequiredHashes:     []string{},
		Artifacts:          map[string]string{},
		DatasetVersionID:   plan.DatasetVersionID,
		ParamsHash:         plan.PlanHash,
		SkillBundleVersion: nil,
		Events: []domain.ExecutionEvent{
			{
				ExecutionID: executionID,
				TS:          now,
				Level:       "info",
				EventType:   "WORKFLOW_ENQUEUED",
				Message:     "execution handed off to Temporal workflow placeholder",
				Payload: map[string]any{
					"job_id":  jobID,
					"plan_id": plan.PlanID,
				},
			},
		},
	}
	workflowID, err := s.starter.StartAnalysisWorkflow(workflows.StartAnalysisInput{
		ExecutionID:      executionID,
		ProjectID:        projectID,
		RequestID:        plan.RequestID,
		PlanID:           plan.PlanID,
		DatasetVersionID: plan.DatasetVersionID,
	})
	if err != nil {
		return domain.PlanExecuteResponse{}, err
	}
	execution.Events[0].Payload["workflow_id"] = workflowID
	execution.Events[0].Payload["workflow_engine"] = s.starter.EngineName()
	if err := s.store.SaveExecution(execution); err != nil {
		return domain.PlanExecuteResponse{}, err
	}

	return domain.PlanExecuteResponse{
		Plan:      plan,
		Execution: execution,
		JobID:     &jobID,
	}, nil
}

func (s *AnalysisService) GetExecution(projectID, executionID string) (domain.ExecutionSummary, error) {
	execution, err := s.store.GetExecution(projectID, executionID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.ExecutionSummary{}, ErrNotFound{Resource: "execution"}
		}
		return domain.ExecutionSummary{}, err
	}
	return execution, nil
}

func (s *AnalysisService) BuildExecutionResult(projectID, executionID string) (domain.ExecutionResultResponse, error) {
	execution, err := s.GetExecution(projectID, executionID)
	if err != nil {
		return domain.ExecutionResultResponse{}, err
	}

	contract := map[string]any{
		"status":          execution.Status,
		"plan_id":         execution.Plan.PlanID,
		"dataset_version": execution.DatasetVersionID,
		"step_count":      len(execution.Plan.Steps),
		"skill_names":     extractSkillNames(execution.Plan),
		"artifact_keys":   sortedArtifactKeys(execution.Artifacts),
		"evidence_artifact_keys": filterArtifactKeysBySkills(
			sortedArtifactKeys(execution.Artifacts),
			"issue_evidence_summary",
			"evidence_pack",
		),
		"reproducibility": map[string]any{
			"params_hash":          execution.ParamsHash,
			"code_version":         execution.CodeVersion,
			"skill_bundle_version": execution.SkillBundleVersion,
		},
	}

	return domain.ExecutionResultResponse{
		ExecutionID: execution.ExecutionID,
		Artifacts:   execution.Artifacts,
		Contract:    contract,
	}, nil
}

func (s *AnalysisService) ResumeExecution(projectID, executionID string, input domain.ExecutionResumeRequest) (domain.ExecutionSummary, error) {
	execution, err := s.GetExecution(projectID, executionID)
	if err != nil {
		return domain.ExecutionSummary{}, err
	}
	if execution.Status != "waiting" {
		return domain.ExecutionSummary{}, ErrInvalidArgument{Message: "only waiting executions can be resumed"}
	}

	now := time.Now().UTC()
	reason := "external dependency is ready"
	if input.Reason != nil && strings.TrimSpace(*input.Reason) != "" {
		reason = strings.TrimSpace(*input.Reason)
	}
	triggeredBy := "manual"
	if input.TriggeredBy != nil && strings.TrimSpace(*input.TriggeredBy) != "" {
		triggeredBy = strings.TrimSpace(*input.TriggeredBy)
	}

	execution.Status = "queued"
	execution.EndedAt = nil
	execution.Events = append(execution.Events, domain.ExecutionEvent{
		ExecutionID: execution.ExecutionID,
		TS:          now,
		Level:       "info",
		EventType:   "RESUME_ENQUEUED",
		Message:     "waiting execution resumed",
		Payload: map[string]any{
			"reason":       reason,
			"triggered_by": triggeredBy,
		},
	})

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
	lastEvent := &execution.Events[len(execution.Events)-1]
	lastEvent.Payload["workflow_id"] = workflowID
	lastEvent.Payload["workflow_engine"] = s.starter.EngineName()

	if err := s.store.SaveExecution(execution); err != nil {
		return domain.ExecutionSummary{}, err
	}
	return execution, nil
}

func (s *AnalysisService) RerunExecution(projectID, executionID string, input domain.ExecutionRerunRequest) (domain.ExecutionRerunResponse, error) {
	execution, err := s.GetExecution(projectID, executionID)
	if err != nil {
		return domain.ExecutionRerunResponse{}, err
	}

	jobID := id.New()
	newExecutionID := id.New()
	now := time.Now().UTC()
	mode := "strict_repro"
	if input.Mode != nil && strings.TrimSpace(*input.Mode) != "" {
		mode = strings.TrimSpace(*input.Mode)
	}

	rerun := domain.ExecutionSummary{
		ExecutionID:        newExecutionID,
		ProjectID:          execution.ProjectID,
		RequestID:          execution.RequestID,
		Plan:               execution.Plan,
		Status:             "queued",
		RequiredHashes:     execution.RequiredHashes,
		EmbeddingModel:     execution.EmbeddingModel,
		Artifacts:          map[string]string{},
		DatasetVersionID:   execution.DatasetVersionID,
		CodeVersion:        execution.CodeVersion,
		ParamsHash:         execution.ParamsHash,
		SkillBundleVersion: execution.SkillBundleVersion,
		Events: []domain.ExecutionEvent{
			{
				ExecutionID: newExecutionID,
				TS:          now,
				Level:       "info",
				EventType:   "RERUN_ENQUEUED",
				Message:     "rerun handed off to Temporal workflow placeholder",
				Payload: map[string]any{
					"source_execution_id": execution.ExecutionID,
					"job_id":              jobID,
					"mode":                mode,
				},
			},
		},
	}
	workflowID, err := s.starter.StartAnalysisWorkflow(workflows.StartAnalysisInput{
		ExecutionID:      newExecutionID,
		ProjectID:        execution.ProjectID,
		RequestID:        execution.RequestID,
		PlanID:           execution.Plan.PlanID,
		DatasetVersionID: execution.DatasetVersionID,
	})
	if err != nil {
		return domain.ExecutionRerunResponse{}, err
	}
	rerun.Events[0].Payload["workflow_id"] = workflowID
	rerun.Events[0].Payload["workflow_engine"] = s.starter.EngineName()
	if err := s.store.SaveExecution(rerun); err != nil {
		return domain.ExecutionRerunResponse{}, err
	}

	return domain.ExecutionRerunResponse{
		Execution: rerun,
		JobID:     &jobID,
	}, nil
}

func (s *AnalysisService) DiffExecutions(projectID, fromExecutionID, toExecutionID string) (domain.ExecutionDiffResponse, error) {
	fromExecution, err := s.GetExecution(projectID, fromExecutionID)
	if err != nil {
		return domain.ExecutionDiffResponse{}, err
	}
	toExecution, err := s.GetExecution(projectID, toExecutionID)
	if err != nil {
		return domain.ExecutionDiffResponse{}, err
	}

	total := len(fromExecution.Plan.Steps)
	if len(toExecution.Plan.Steps) > total {
		total = len(toExecution.Plan.Steps)
	}

	steps := make([]domain.ExecutionDiffStep, 0, total)
	changed := 0
	for index := 0; index < total; index++ {
		fromStep, fromOK := planStepAt(fromExecution.Plan, index)
		toStep, toOK := planStepAt(toExecution.Plan, index)

		stepID := fmt.Sprintf("step:%d", index)
		skillName := "missing"
		status := "unchanged"
		if fromOK {
			stepID = fromStep.StepID
			skillName = fromStep.SkillName
		}
		if toOK && skillName == "missing" {
			stepID = toStep.StepID
			skillName = toStep.SkillName
		}

		var fromHash *string
		var toHash *string
		if fromOK {
			value := computeStepHash(fromStep)
			fromHash = &value
		}
		if toOK {
			value := computeStepHash(toStep)
			toHash = &value
		}
		if !fromOK || !toOK || derefString(fromHash) != derefString(toHash) {
			status = "changed"
			changed++
		}

		steps = append(steps, domain.ExecutionDiffStep{
			StepID:    stepID,
			SkillName: skillName,
			Status:    status,
			FromHash:  fromHash,
			ToHash:    toHash,
			Stats: map[string]any{
				"from_present": fromOK,
				"to_present":   toOK,
			},
		})
	}

	return domain.ExecutionDiffResponse{
		FromExecutionID: fromExecutionID,
		ToExecutionID:   toExecutionID,
		TotalSteps:      total,
		ChangedSteps:    changed,
		Steps:           steps,
	}, nil
}

func buildDefaultPlan(input domain.AnalysisSubmitRequest) domain.SkillPlan {
	now := time.Now().UTC()
	dataType := "structured"
	if input.DataType != nil && strings.TrimSpace(*input.DataType) != "" {
		dataType = strings.TrimSpace(*input.DataType)
	}
	skills := registry.MVPDefaultSkills
	if selected, ok := registry.MVPSkillsByDataType[dataType]; ok && len(selected) > 0 {
		skills = selected
	}

	datasetName := "dataset_from_version"
	if input.DatasetName != nil && strings.TrimSpace(*input.DatasetName) != "" {
		datasetName = strings.TrimSpace(*input.DatasetName)
	}

	notes := "generated by control-plane scaffold"
	steps := make([]domain.SkillPlanStep, 0, len(skills))
	for _, skillName := range skills {
		steps = append(steps, domain.SkillPlanStep{
			StepID:      id.New(),
			SkillName:   skillName,
			DatasetName: datasetName,
			Inputs:      defaultInputsForSkill(skillName, input.Goal),
		})
	}

	return domain.SkillPlan{
		PlanID:    id.New(),
		Steps:     steps,
		Notes:     &notes,
		CreatedAt: now,
	}
}

func normalizePlan(plan domain.SkillPlan, datasetName string, version domain.DatasetVersion, goal string) domain.SkillPlan {
	if strings.TrimSpace(plan.PlanID) == "" {
		plan.PlanID = id.New()
	}
	if plan.CreatedAt.IsZero() {
		plan.CreatedAt = time.Now().UTC()
	}
	for index := range plan.Steps {
		if strings.TrimSpace(plan.Steps[index].StepID) == "" {
			plan.Steps[index].StepID = id.New()
		}
		if strings.TrimSpace(plan.Steps[index].DatasetName) == "" || strings.TrimSpace(plan.Steps[index].DatasetName) == "dataset_from_version" || strings.TrimSpace(plan.Steps[index].DatasetName) == datasetName {
			plan.Steps[index].DatasetName = resolvedDatasetNameForSkill(plan.Steps[index].SkillName, datasetName, version)
		}
		if plan.Steps[index].Inputs == nil {
			plan.Steps[index].Inputs = map[string]any{}
		}
		enrichInputsForSkill(&plan.Steps[index], version, goal)
	}
	return plan
}

func computePlanHash(plan domain.SkillPlan) string {
	payload, err := json.Marshal(plan)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func computeStepHash(step domain.SkillPlanStep) string {
	payload, err := json.Marshal(step)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func planStepAt(plan domain.SkillPlan, index int) (domain.SkillPlanStep, bool) {
	if index < 0 || index >= len(plan.Steps) {
		return domain.SkillPlanStep{}, false
	}
	return plan.Steps[index], true
}

func extractSkillNames(plan domain.SkillPlan) []string {
	names := make([]string, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		names = append(names, step.SkillName)
	}
	return names
}

func defaultInputsForSkill(skillName, goal string) map[string]any {
	inputs := map[string]any{}
	switch skillName {
	case "structured_kpi_summary":
		inputs["time_column"] = "date"
		inputs["metric_column"] = "value"
	case "document_filter":
		inputs["text_column"] = "text"
		inputs["sample_n"] = 5
		if strings.TrimSpace(goal) != "" {
			inputs["query"] = strings.TrimSpace(goal)
		}
	case "keyword_frequency":
		inputs["text_column"] = "text"
		inputs["top_n"] = 10
	case "time_bucket_count":
		inputs["text_column"] = "text"
		inputs["time_column"] = "occurred_at"
		inputs["bucket"] = "day"
		inputs["top_n"] = 5
		inputs["sample_n"] = 3
	case "meta_group_count":
		inputs["text_column"] = "text"
		inputs["dimension_column"] = "channel"
		inputs["top_n"] = 5
		inputs["sample_n"] = 3
	case "document_sample":
		inputs["text_column"] = "text"
		inputs["sample_n"] = 3
		if strings.TrimSpace(goal) != "" {
			inputs["query"] = strings.TrimSpace(goal)
		}
	case "unstructured_issue_summary":
		inputs["text_column"] = "text"
		inputs["top_n"] = 10
		inputs["sample_n"] = 3
	case "issue_breakdown_summary":
		inputs["text_column"] = "text"
		inputs["dimension_column"] = "channel"
		inputs["top_n"] = 5
		inputs["sample_n"] = 3
	case "issue_trend_summary":
		inputs["text_column"] = "text"
		inputs["time_column"] = "occurred_at"
		inputs["bucket"] = "day"
		inputs["top_n"] = 5
		inputs["sample_n"] = 3
	case "issue_period_compare":
		inputs["text_column"] = "text"
		inputs["time_column"] = "occurred_at"
		inputs["bucket"] = "day"
		inputs["window_size"] = 1
		inputs["top_n"] = 5
		inputs["sample_n"] = 3
	case "issue_sentiment_summary":
		inputs["text_column"] = "text"
		inputs["sentiment_column"] = "sentiment_label"
		inputs["sample_n"] = 3
	case "issue_evidence_summary":
		inputs["text_column"] = "text"
		inputs["sample_n"] = 3
		if strings.TrimSpace(goal) != "" {
			inputs["query"] = strings.TrimSpace(goal)
		}
	case "semantic_search":
		inputs["text_column"] = "text"
		inputs["sample_n"] = 5
		if strings.TrimSpace(goal) != "" {
			inputs["query"] = strings.TrimSpace(goal)
		}
	case "evidence_pack":
		inputs["text_column"] = "text"
		inputs["sample_n"] = 3
		if strings.TrimSpace(goal) != "" {
			inputs["query"] = strings.TrimSpace(goal)
		}
	}
	return inputs
}

func enrichInputsForSkill(step *domain.SkillPlanStep, version domain.DatasetVersion, goal string) {
	if step.Inputs == nil {
		step.Inputs = map[string]any{}
	}
	textColumn := resolvedTextColumnForSkill(step.Inputs, version)
	switch strings.TrimSpace(step.SkillName) {
	case "document_filter":
		step.Inputs["text_column"] = textColumn
		if _, ok := step.Inputs["sample_n"]; !ok {
			step.Inputs["sample_n"] = 5
		}
		if _, ok := step.Inputs["query"]; !ok && strings.TrimSpace(goal) != "" {
			step.Inputs["query"] = strings.TrimSpace(goal)
		}
	case "keyword_frequency":
		step.Inputs["text_column"] = textColumn
		if _, ok := step.Inputs["top_n"]; !ok {
			step.Inputs["top_n"] = 10
		}
	case "time_bucket_count":
		step.Inputs["text_column"] = textColumn
		if _, ok := step.Inputs["time_column"]; !ok {
			step.Inputs["time_column"] = metadataString(version.Metadata, "time_column", "occurred_at")
		}
		if _, ok := step.Inputs["bucket"]; !ok {
			step.Inputs["bucket"] = metadataString(version.Metadata, "time_bucket", "day")
		}
		if _, ok := step.Inputs["top_n"]; !ok {
			step.Inputs["top_n"] = 5
		}
		if _, ok := step.Inputs["sample_n"]; !ok {
			step.Inputs["sample_n"] = 3
		}
	case "meta_group_count":
		step.Inputs["text_column"] = textColumn
		if _, ok := step.Inputs["dimension_column"]; !ok {
			step.Inputs["dimension_column"] = metadataString(version.Metadata, "breakdown_column", "channel")
		}
		if _, ok := step.Inputs["top_n"]; !ok {
			step.Inputs["top_n"] = 5
		}
		if _, ok := step.Inputs["sample_n"]; !ok {
			step.Inputs["sample_n"] = 3
		}
	case "document_sample":
		step.Inputs["text_column"] = textColumn
		if _, ok := step.Inputs["sample_n"]; !ok {
			step.Inputs["sample_n"] = 3
		}
		if _, ok := step.Inputs["query"]; !ok && strings.TrimSpace(goal) != "" {
			step.Inputs["query"] = strings.TrimSpace(goal)
		}
	case "unstructured_issue_summary":
		step.Inputs["text_column"] = textColumn
		if _, ok := step.Inputs["top_n"]; !ok {
			step.Inputs["top_n"] = 10
		}
		if _, ok := step.Inputs["sample_n"]; !ok {
			step.Inputs["sample_n"] = 3
		}
	case "issue_breakdown_summary":
		step.Inputs["text_column"] = textColumn
		if _, ok := step.Inputs["dimension_column"]; !ok {
			step.Inputs["dimension_column"] = metadataString(version.Metadata, "breakdown_column", "channel")
		}
		if _, ok := step.Inputs["top_n"]; !ok {
			step.Inputs["top_n"] = 5
		}
		if _, ok := step.Inputs["sample_n"]; !ok {
			step.Inputs["sample_n"] = 3
		}
	case "issue_trend_summary":
		step.Inputs["text_column"] = textColumn
		if _, ok := step.Inputs["time_column"]; !ok {
			step.Inputs["time_column"] = metadataString(version.Metadata, "time_column", "occurred_at")
		}
		if _, ok := step.Inputs["bucket"]; !ok {
			step.Inputs["bucket"] = metadataString(version.Metadata, "time_bucket", "day")
		}
		if _, ok := step.Inputs["top_n"]; !ok {
			step.Inputs["top_n"] = 5
		}
		if _, ok := step.Inputs["sample_n"]; !ok {
			step.Inputs["sample_n"] = 3
		}
	case "issue_period_compare":
		step.Inputs["text_column"] = textColumn
		if _, ok := step.Inputs["time_column"]; !ok {
			step.Inputs["time_column"] = metadataString(version.Metadata, "time_column", "occurred_at")
		}
		if _, ok := step.Inputs["bucket"]; !ok {
			step.Inputs["bucket"] = metadataString(version.Metadata, "time_bucket", "day")
		}
		if _, ok := step.Inputs["window_size"]; !ok {
			step.Inputs["window_size"] = 1
		}
		if _, ok := step.Inputs["top_n"]; !ok {
			step.Inputs["top_n"] = 5
		}
		if _, ok := step.Inputs["sample_n"]; !ok {
			step.Inputs["sample_n"] = 3
		}
	case "issue_sentiment_summary":
		step.Inputs["text_column"] = textColumn
		if _, ok := step.Inputs["sentiment_column"]; !ok {
			step.Inputs["sentiment_column"] = metadataString(version.Metadata, "sentiment_label_column", "sentiment_label")
		}
		if _, ok := step.Inputs["sample_n"]; !ok {
			step.Inputs["sample_n"] = 3
		}
	case "semantic_search":
		step.Inputs["text_column"] = textColumn
		if _, ok := step.Inputs["sample_n"]; !ok {
			step.Inputs["sample_n"] = 5
		}
		if _, ok := step.Inputs["query"]; !ok && strings.TrimSpace(goal) != "" {
			step.Inputs["query"] = strings.TrimSpace(goal)
		}
		if _, ok := step.Inputs["embedding_uri"]; !ok {
			step.Inputs["embedding_uri"] = deriveEmbeddingURI(version)
		}
	case "evidence_pack":
		fallthrough
	case "issue_evidence_summary":
		step.Inputs["text_column"] = textColumn
		if _, ok := step.Inputs["sample_n"]; !ok {
			step.Inputs["sample_n"] = 3
		}
		if _, ok := step.Inputs["query"]; !ok && strings.TrimSpace(goal) != "" {
			step.Inputs["query"] = strings.TrimSpace(goal)
		}
	}
}

func resolvedDatasetNameForSkill(skillName, fallback string, version domain.DatasetVersion) string {
	if usesSentimentDataset(skillName) {
		return datasetSourceForSentiment(version)
	}
	if usesPreparedDataset(skillName) {
		return datasetSourceForUnstructured(version)
	}
	return fallback
}

func defaultTextColumnForSkill(version domain.DatasetVersion) string {
	return defaultPreparedTextColumn(version)
}

func resolvedTextColumnForSkill(inputs map[string]any, version domain.DatasetVersion) string {
	defaultTextColumn := defaultTextColumnForSkill(version)
	if !isPrepareReady(version) {
		if inputs == nil {
			return defaultTextColumn
		}
		if value, ok := inputs["text_column"]; ok {
			text := strings.TrimSpace(fmt.Sprintf("%v", value))
			if text != "" {
				return text
			}
		}
		return defaultTextColumn
	}

	rawTextColumn := metadataString(version.Metadata, "raw_text_column", metadataString(version.Metadata, "text_column", "text"))
	if inputs == nil {
		return defaultTextColumn
	}
	value, ok := inputs["text_column"]
	if !ok {
		return defaultTextColumn
	}
	text := strings.TrimSpace(fmt.Sprintf("%v", value))
	if text == "" || text == rawTextColumn {
		return defaultTextColumn
	}
	return text
}

func usesPreparedDataset(skillName string) bool {
	switch strings.TrimSpace(skillName) {
	case "document_filter", "keyword_frequency", "time_bucket_count", "meta_group_count", "document_sample", "unstructured_issue_summary", "issue_breakdown_summary", "issue_trend_summary", "issue_period_compare", "semantic_search", "issue_evidence_summary", "evidence_pack":
		return true
	default:
		return false
	}
}

func usesSentimentDataset(skillName string) bool {
	switch strings.TrimSpace(skillName) {
	case "issue_sentiment_summary":
		return true
	default:
		return false
	}
}

func sortedArtifactKeys(artifacts map[string]string) []string {
	keys := make([]string, 0, len(artifacts))
	for key := range artifacts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func filterArtifactKeysBySkill(keys []string, skillName string) []string {
	return filterArtifactKeysBySkills(keys, skillName)
}

func filterArtifactKeysBySkills(keys []string, skillNames ...string) []string {
	filtered := make([]string, 0, len(keys))
	suffixes := make([]string, 0, len(skillNames))
	for _, skillName := range skillNames {
		skillName = strings.TrimSpace(skillName)
		if skillName == "" {
			continue
		}
		suffixes = append(suffixes, ":"+skillName)
	}
	for _, key := range keys {
		for _, suffix := range suffixes {
			if strings.HasSuffix(key, suffix) {
				filtered = append(filtered, key)
				break
			}
		}
	}
	return filtered
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func metadataString(metadata map[string]any, key, fallback string) string {
	if metadata == nil {
		return fallback
	}
	value, ok := metadata[key]
	if !ok {
		return fallback
	}
	text := strings.TrimSpace(fmt.Sprintf("%v", value))
	if text == "" {
		return fallback
	}
	return text
}
