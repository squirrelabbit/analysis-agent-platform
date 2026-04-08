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
	"analysis-support-platform/control-plane/internal/executionresult"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/planner"
	"analysis-support-platform/control-plane/internal/registry"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"
)

type AnalysisService struct {
	store             store.Repository
	starter           workflows.Starter
	planner           planner.Planner
	dependencyBuilder executionDependencyBuilder
}

type executionDependencyBuilder interface {
	CreatePrepareJob(projectID, datasetID, datasetVersionID string, input domain.DatasetPrepareRequest, triggeredBy string) (domain.DatasetBuildJob, error)
	CreateSentimentJob(projectID, datasetID, datasetVersionID string, input domain.DatasetSentimentBuildRequest, triggeredBy string) (domain.DatasetBuildJob, error)
	CreateEmbeddingJob(projectID, datasetID, datasetVersionID string, input domain.DatasetEmbeddingBuildRequest, triggeredBy string) (domain.DatasetBuildJob, error)
	CreateClusterJob(projectID, datasetID, datasetVersionID string, input domain.DatasetClusterBuildRequest, triggeredBy string) (domain.DatasetBuildJob, error)
}

func NewAnalysisService(repository store.Repository, starter workflows.Starter, planGenerator planner.Planner) *AnalysisService {
	return &AnalysisService{
		store:   repository,
		starter: starter,
		planner: planGenerator,
	}
}

func (s *AnalysisService) SetDependencyBuilder(builder executionDependencyBuilder) {
	s.dependencyBuilder = builder
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
	if err := s.ensureExecutionDependencies(projectID, plan); err != nil {
		return domain.PlanExecuteResponse{}, err
	}

	executionID := id.New()
	jobID := id.New()
	now := time.Now().UTC()
	var profileSnapshot *domain.DatasetProfile
	if plan.DatasetVersionID != nil && strings.TrimSpace(*plan.DatasetVersionID) != "" {
		version, err := s.store.GetDatasetVersion(projectID, strings.TrimSpace(*plan.DatasetVersionID))
		if err != nil {
			if err == store.ErrNotFound {
				return domain.PlanExecuteResponse{}, ErrNotFound{Resource: "dataset version"}
			}
			return domain.PlanExecuteResponse{}, err
		}
		profileSnapshot = cloneDatasetProfile(version.Profile)
		plan.Plan = refreshPlanWithDatasetVersion(plan.Plan, version)
	}
	execution := domain.ExecutionSummary{
		ExecutionID:        executionID,
		ProjectID:          projectID,
		RequestID:          plan.RequestID,
		Plan:               plan.Plan,
		Status:             "queued",
		CreatedAt:          now,
		RequiredHashes:     []string{},
		Artifacts:          map[string]string{},
		DatasetVersionID:   plan.DatasetVersionID,
		ParamsHash:         plan.PlanHash,
		SkillBundleVersion: stringPointer(registry.BundleVersion()),
		ProfileSnapshot:    profileSnapshot,
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

func (s *AnalysisService) ensureExecutionDependencies(projectID string, plan domain.PlanRecord) error {
	if s.dependencyBuilder == nil || plan.DatasetVersionID == nil || strings.TrimSpace(*plan.DatasetVersionID) == "" {
		return nil
	}

	versionID := strings.TrimSpace(*plan.DatasetVersionID)
	version, err := s.store.GetDatasetVersion(projectID, versionID)
	if err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "dataset version"}
		}
		return err
	}

	_, err = s.ensureExecutionDependenciesForVersion(projectID, version, plan.Plan, "analysis_execute")
	return err
}

func (s *AnalysisService) ensureExecutionDependenciesForVersion(projectID string, version domain.DatasetVersion, plan domain.SkillPlan, triggeredBy string) (domain.DatasetVersion, error) {
	if s.dependencyBuilder == nil {
		return version, nil
	}

	versionID := strings.TrimSpace(version.DatasetVersionID)
	needsPrepare := planRequiresPrepare(plan)
	needsSentiment := planRequiresSentiment(plan)
	needsEmbedding := planRequiresEmbedding(plan)
	needsCluster := planRequiresCluster(plan)
	clusterRequest, hasMaterializedClusterRequest := domain.ClusterMaterializationRequestForPlan(plan)

	if needsPrepare && requiresPrepare(version) && !datasetPrepareReady(version) {
		if _, err := s.dependencyBuilder.CreatePrepareJob(projectID, version.DatasetID, version.DatasetVersionID, domain.DatasetPrepareRequest{}, triggeredBy); err != nil {
			return domain.DatasetVersion{}, err
		}
		latest, err := s.store.GetDatasetVersion(projectID, versionID)
		if err != nil {
			return domain.DatasetVersion{}, err
		}
		return latest, nil
	}
	if needsSentiment && !datasetSentimentReady(version) {
		if _, err := s.dependencyBuilder.CreateSentimentJob(projectID, version.DatasetID, version.DatasetVersionID, domain.DatasetSentimentBuildRequest{}, triggeredBy); err != nil {
			return domain.DatasetVersion{}, err
		}
		latest, err := s.store.GetDatasetVersion(projectID, versionID)
		if err != nil {
			return domain.DatasetVersion{}, err
		}
		return latest, nil
	}
	if needsEmbedding && !datasetEmbeddingReady(version) {
		if _, err := s.dependencyBuilder.CreateEmbeddingJob(projectID, version.DatasetID, version.DatasetVersionID, domain.DatasetEmbeddingBuildRequest{}, triggeredBy); err != nil {
			return domain.DatasetVersion{}, err
		}
		latest, err := s.store.GetDatasetVersion(projectID, versionID)
		if err != nil {
			return domain.DatasetVersion{}, err
		}
		return latest, nil
	}
	if needsCluster && hasMaterializedClusterRequest && clusterRequest != nil && !domain.ClusterRequestMatchesMetadata(*clusterRequest, version.Metadata) {
		if _, err := s.dependencyBuilder.CreateClusterJob(projectID, version.DatasetID, version.DatasetVersionID, *clusterRequest, triggeredBy); err != nil {
			return domain.DatasetVersion{}, err
		}
		latest, err := s.store.GetDatasetVersion(projectID, versionID)
		if err != nil {
			return domain.DatasetVersion{}, err
		}
		return latest, nil
	}

	return version, nil
}

func (s *AnalysisService) SubmitAndExecute(projectID string, input domain.AnalysisSubmitRequest) (domain.AnalysisExecuteResponse, error) {
	planned, err := s.SubmitAnalysis(projectID, input)
	if err != nil {
		return domain.AnalysisExecuteResponse{}, err
	}
	executed, err := s.ExecutePlan(projectID, planned.Plan.PlanID)
	if err != nil {
		return domain.AnalysisExecuteResponse{}, err
	}
	return domain.AnalysisExecuteResponse{
		Request:   planned.Request,
		Plan:      executed.Plan,
		Execution: executed.Execution,
		JobID:     executed.JobID,
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
	return withExecutionDiagnostics(execution), nil
}

func (s *AnalysisService) ListExecutions(projectID string) (domain.ExecutionListResponse, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ExecutionListResponse{}, ErrNotFound{Resource: "project"}
		}
		return domain.ExecutionListResponse{}, err
	}
	executions, err := s.store.ListExecutions(projectID)
	if err != nil {
		return domain.ExecutionListResponse{}, err
	}
	items := make([]domain.ExecutionListItem, 0, len(executions))
	for _, execution := range executions {
		execution = withExecutionDiagnostics(execution)
		item := executionresult.BuildListItem(execution)
		item.Diagnostics = execution.Diagnostics
		items = append(items, item)
	}
	return domain.ExecutionListResponse{Items: items}, nil
}

func (s *AnalysisService) BuildExecutionResult(projectID, executionID string) (domain.ExecutionResultResponse, error) {
	execution, err := s.GetExecution(projectID, executionID)
	if err != nil {
		return domain.ExecutionResultResponse{}, err
	}
	resultV1 := buildExecutionResultV1(execution, nil)

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
	if usageSummary := buildArtifactUsageSummary(execution.Artifacts); len(usageSummary) > 0 {
		contract["usage_summary"] = usageSummary
	}
	if stepHooks := latestCompletedStepHooks(execution.Events); stepHooks != nil {
		contract["step_hooks"] = stepHooks
	}

	return domain.ExecutionResultResponse{
		ExecutionID: execution.ExecutionID,
		Artifacts:   execution.Artifacts,
		Contract:    contract,
		ResultV1:    resultV1,
		FinalAnswer: executionresult.BuildFinalAnswer(execution),
		Diagnostics: execution.Diagnostics,
	}, nil
}

func (s *AnalysisService) CreateReportDraft(projectID string, input domain.ReportDraftCreateRequest) (domain.ReportDraft, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ReportDraft{}, ErrNotFound{Resource: "project"}
		}
		return domain.ReportDraft{}, err
	}
	if len(input.ExecutionIDs) == 0 {
		return domain.ReportDraft{}, ErrInvalidArgument{Message: "execution_ids is required"}
	}
	executionIDs := uniqueNonEmptyStrings(input.ExecutionIDs)
	if len(executionIDs) == 0 {
		return domain.ReportDraft{}, ErrInvalidArgument{Message: "execution_ids is required"}
	}
	executions := make([]domain.ExecutionSummary, 0, len(executionIDs))
	for _, executionID := range executionIDs {
		execution, err := s.GetExecution(projectID, executionID)
		if err != nil {
			return domain.ReportDraft{}, err
		}
		executions = append(executions, execution)
	}
	title := strings.TrimSpace(optionalStringValue(input.Title))
	if title == "" {
		title = "실행 결과 보고서 초안"
	}
	draft := domain.ReportDraft{
		DraftID:      id.New(),
		ProjectID:    projectID,
		Title:        title,
		ExecutionIDs: executionIDs,
		Content:      executionresult.BuildReportDraftV1(title, executions),
		CreatedAt:    time.Now().UTC(),
	}
	if err := s.store.SaveReportDraft(draft); err != nil {
		return domain.ReportDraft{}, err
	}
	return draft, nil
}

func withExecutionDiagnostics(execution domain.ExecutionSummary) domain.ExecutionSummary {
	diagnostics := &domain.ExecutionDiagnostics{
		EventCount: len(execution.Events),
	}
	if execution.FinalAnswerSnapshot != nil {
		diagnostics.FinalAnswerStatus = strings.TrimSpace(execution.FinalAnswerSnapshot.Status)
		if diagnostics.FinalAnswerStatus == "" {
			diagnostics.FinalAnswerStatus = "ready"
		}
	} else if execution.FinalAnswerError != nil && strings.TrimSpace(*execution.FinalAnswerError) != "" {
		diagnostics.FinalAnswerStatus = "failed"
		diagnostics.FinalAnswerError = strings.TrimSpace(*execution.FinalAnswerError)
	} else if strings.TrimSpace(execution.Status) == "completed" {
		diagnostics.FinalAnswerStatus = "pending"
	}
	if len(execution.Events) > 0 {
		latest := execution.Events[len(execution.Events)-1]
		diagnostics.LatestEventType = strings.TrimSpace(latest.EventType)
		diagnostics.LatestEventMessage = strings.TrimSpace(latest.Message)
	}
	for index := len(execution.Events) - 1; index >= 0; index-- {
		event := execution.Events[index]
		if event.EventType == "WORKFLOW_FAILED" {
			failureReason := strings.TrimSpace(event.Message)
			if failureReason == "" {
				failureReason = strings.TrimSpace(anyStringValue(event.Payload["error"]))
			}
			diagnostics.FailureReason = failureReason
			break
		}
	}
	for index := len(execution.Events) - 1; index >= 0; index-- {
		event := execution.Events[index]
		if event.EventType != "WORKFLOW_WAITING" {
			continue
		}
		waitingFor := strings.TrimSpace(anyStringValue(event.Payload["waiting_for"]))
		reason := strings.TrimSpace(anyStringValue(event.Payload["reason"]))
		if waitingFor == "" && reason == "" {
			continue
		}
		diagnostics.Waiting = &domain.ExecutionWaitingState{
			WaitingFor: waitingFor,
			Reason:     reason,
		}
		break
	}
	execution.Diagnostics = diagnostics
	return execution
}

func (s *AnalysisService) GetReportDraft(projectID, draftID string) (domain.ReportDraft, error) {
	draft, err := s.store.GetReportDraft(projectID, draftID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.ReportDraft{}, ErrNotFound{Resource: "report draft"}
		}
		return domain.ReportDraft{}, err
	}
	return draft, nil
}

func buildExecutionResultV1(execution domain.ExecutionSummary, contract map[string]any) domain.ExecutionResultV1 {
	if execution.ResultV1Snapshot != nil {
		result := *execution.ResultV1Snapshot
		if result.Profile == nil && execution.ProfileSnapshot != nil {
			result.Profile = cloneDatasetProfile(execution.ProfileSnapshot)
		}
		return result
	}
	return executionresult.BuildV1(execution)
}

func optionalStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func anyStringValue(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case *string:
		return optionalStringValue(typed)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func latestCompletedStepHooks(events []domain.ExecutionEvent) any {
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if event.EventType != "WORKFLOW_COMPLETED" {
			continue
		}
		if hooks, ok := event.Payload["step_hooks"]; ok {
			return hooks
		}
	}
	return nil
}

func buildArtifactUsageSummary(artifacts map[string]string) map[string]any {
	summary := map[string]any{}
	for _, raw := range artifacts {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
			continue
		}
		usage, ok := decoded["usage"].(map[string]any)
		if !ok || len(usage) == 0 {
			continue
		}
		summary = mergeExecutionUsage(summary, usage)
	}
	if len(summary) == 0 {
		return nil
	}
	return summary
}

func mergeExecutionUsage(left, right map[string]any) map[string]any {
	if len(left) == 0 && len(right) == 0 {
		return nil
	}
	result := map[string]any{}
	for key, value := range left {
		result[key] = value
	}
	for key, value := range right {
		switch key {
		case "request_count", "input_tokens", "output_tokens", "total_tokens", "prompt_tokens", "input_text_count", "vector_count":
			result[key] = usageIntValue(result[key]) + usageIntValue(value)
		case "estimated_cost_usd":
			result[key] = usageRoundCost(usageFloatValue(result[key]) + usageFloatValue(value))
		case "provider", "model", "operation", "cost_estimation_status":
			existing := strings.TrimSpace(artifactStringValue(result[key]))
			incoming := strings.TrimSpace(artifactStringValue(value))
			if existing == "" {
				result[key] = incoming
			} else if incoming == "" || existing == incoming {
				result[key] = existing
			} else {
				result[key] = "mixed"
			}
		default:
			if _, ok := result[key]; !ok {
				result[key] = value
			}
		}
	}
	return result
}

func usageIntValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

func usageFloatValue(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	default:
		return 0
	}
}

func decodeExecutionArtifacts(artifacts map[string]string) map[string]map[string]any {
	decoded := make(map[string]map[string]any, len(artifacts))
	for key, raw := range artifacts {
		var artifact map[string]any
		if err := json.Unmarshal([]byte(raw), &artifact); err != nil {
			continue
		}
		decoded[key] = artifact
	}
	return decoded
}

func selectPrimaryExecutionArtifact(decoded map[string]map[string]any, plan domain.SkillPlan) (string, map[string]any) {
	priority := []string{
		"issue_evidence_summary",
		"evidence_pack",
		"issue_cluster_summary",
		"issue_taxonomy_summary",
		"issue_sentiment_summary",
		"issue_breakdown_summary",
		"issue_trend_summary",
		"issue_period_compare",
		"structured_kpi_summary",
		"unstructured_issue_summary",
		"semantic_search",
	}
	keys := sortedArtifactKeysFromDecoded(decoded)
	for _, skillName := range priority {
		for _, step := range plan.Steps {
			if step.SkillName != skillName {
				continue
			}
			key := artifactKeyForStep(keys, step.StepID, skillName)
			if key == "" {
				continue
			}
			return key, decoded[key]
		}
		for _, key := range keys {
			if strings.HasSuffix(key, ":"+skillName) {
				return key, decoded[key]
			}
		}
	}
	for _, key := range keys {
		return key, decoded[key]
	}
	return "", nil
}

func buildExecutionAnswerV1(primaryArtifact map[string]any, decoded map[string]map[string]any) *domain.ExecutionResultAnswer {
	if len(primaryArtifact) == 0 {
		return nil
	}
	answer := &domain.ExecutionResultAnswer{
		Summary:           strings.TrimSpace(executionArtifactSummary(primaryArtifact)),
		KeyFindings:       executionArtifactKeyFindings(primaryArtifact),
		Evidence:          executionArtifactEvidence(primaryArtifact),
		FollowUpQuestions: executionArtifactStringList(primaryArtifact["follow_up_questions"]),
		SelectionSource:   strings.TrimSpace(artifactStringValue(primaryArtifact["selection_source"])),
		CitationMode:      strings.TrimSpace(artifactStringValue(primaryArtifact["citation_mode"])),
	}
	if answer.Summary == "" {
		answer.Summary = "실행은 완료됐지만 대표 요약을 생성하지 못했습니다."
	}
	if len(answer.Evidence) == 0 {
		if evidenceKey, evidenceArtifact := selectPrimaryBySkills(decoded, "issue_evidence_summary", "evidence_pack"); evidenceKey != "" {
			answer.Evidence = executionArtifactEvidence(evidenceArtifact)
			if answer.SelectionSource == "" {
				answer.SelectionSource = strings.TrimSpace(artifactStringValue(evidenceArtifact["selection_source"]))
			}
			if answer.CitationMode == "" {
				answer.CitationMode = strings.TrimSpace(artifactStringValue(evidenceArtifact["citation_mode"]))
			}
			if len(answer.FollowUpQuestions) == 0 {
				answer.FollowUpQuestions = executionArtifactStringList(evidenceArtifact["follow_up_questions"])
			}
		}
	}
	if len(answer.KeyFindings) == 0 {
		answer.KeyFindings = deriveExecutionFindings(primaryArtifact)
	}
	return answer
}

func buildExecutionStepResultsV1(execution domain.ExecutionSummary, decoded map[string]map[string]any) []domain.ExecutionStepResultV1 {
	results := make([]domain.ExecutionStepResultV1, 0, len(execution.Plan.Steps))
	keys := sortedArtifactKeysFromDecoded(decoded)
	for _, step := range execution.Plan.Steps {
		result := domain.ExecutionStepResultV1{
			StepID:    step.StepID,
			SkillName: step.SkillName,
			Status:    "pending",
		}
		key := artifactKeyForStep(keys, step.StepID, step.SkillName)
		if key != "" {
			artifact := decoded[key]
			result.Status = "completed"
			result.ArtifactKey = stringPointer(key)
			result.Summary = executionArtifactSummary(artifact)
			result.Usage = executionUsageMap(artifact)
			if artifactRef := firstArtifactRef(artifact); artifactRef != "" {
				result.ArtifactRef = stringPointer(artifactRef)
			}
			if selectionMode := strings.TrimSpace(artifactStringValue(artifact["selection_source"])); selectionMode != "" {
				result.SelectionMode = selectionMode
			}
			if warnings := artifactWarnings(artifact); len(warnings) > 0 {
				result.Warnings = warnings
			}
		} else if execution.Status == "failed" {
			result.Status = "missing"
		}
		results = append(results, result)
	}
	return results
}

func latestWaitingState(status string, events []domain.ExecutionEvent) *domain.ExecutionWaitingState {
	if strings.TrimSpace(status) != "waiting" {
		return nil
	}
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if event.EventType != "WORKFLOW_WAITING" {
			continue
		}
		waitingFor := strings.TrimSpace(artifactStringValue(event.Payload["waiting_for"]))
		reason := strings.TrimSpace(artifactStringValue(event.Payload["reason"]))
		if waitingFor == "" && reason == "" {
			return nil
		}
		return &domain.ExecutionWaitingState{
			WaitingFor: waitingFor,
			Reason:     reason,
		}
	}
	return nil
}

func collectExecutionWarnings(status string, events []domain.ExecutionEvent, decoded map[string]map[string]any) []string {
	warnings := make([]string, 0)
	for _, event := range events {
		switch event.EventType {
		case "WORKFLOW_FAILED":
			message := strings.TrimSpace(event.Message)
			if message != "" {
				warnings = append(warnings, message)
			}
			if errText := strings.TrimSpace(artifactStringValue(event.Payload["error"])); errText != "" {
				warnings = append(warnings, errText)
			}
		case "WORKFLOW_WAITING":
			if strings.TrimSpace(status) != "waiting" {
				continue
			}
			waitingFor := strings.TrimSpace(artifactStringValue(event.Payload["waiting_for"]))
			reason := strings.TrimSpace(artifactStringValue(event.Payload["reason"]))
			if waitingFor != "" || reason != "" {
				warnings = append(warnings, strings.TrimSpace("waiting_for="+waitingFor+" "+reason))
			}
		case "WORKFLOW_COMPLETED":
			for _, note := range executionArtifactStringList(event.Payload["structured_notes"]) {
				lower := strings.ToLower(note)
				if strings.Contains(lower, "fallback") || strings.Contains(lower, "warning") || strings.Contains(lower, "error") || strings.Contains(lower, "failed") {
					warnings = append(warnings, note)
				}
			}
		}
	}
	for _, key := range sortedArtifactKeysFromDecoded(decoded) {
		warnings = append(warnings, artifactWarnings(decoded[key])...)
	}
	return uniqueNonEmptyStrings(warnings)
}

func executionArtifactSummary(artifact map[string]any) string {
	if len(artifact) == 0 {
		return ""
	}
	if summaryText := strings.TrimSpace(artifactStringValue(artifact["summary"])); summaryText != "" && summaryText != "map[]" {
		if _, ok := artifact["summary"].(map[string]any); !ok {
			return summaryText
		}
	}
	skillName := strings.TrimSpace(artifactStringValue(artifact["skill_name"]))
	summary, _ := artifact["summary"].(map[string]any)
	switch skillName {
	case "issue_cluster_summary":
		label := strings.TrimSpace(artifactStringValue(summary["dominant_cluster_label"]))
		count := usageIntValue(summary["dominant_cluster_count"])
		clusterCount := usageIntValue(summary["cluster_count"])
		if label != "" {
			return fmt.Sprintf("가장 큰 군집은 %s이며 %d건입니다. 전체 군집 수는 %d개입니다.", label, count, clusterCount)
		}
	case "issue_taxonomy_summary":
		label := strings.TrimSpace(artifactStringValue(summary["dominant_taxonomy_label"]))
		if label == "" {
			label = strings.TrimSpace(artifactStringValue(summary["dominant_taxonomy"]))
		}
		count := usageIntValue(summary["dominant_taxonomy_count"])
		if label != "" {
			return fmt.Sprintf("가장 큰 taxonomy는 %s이며 %d건입니다.", label, count)
		}
	case "issue_sentiment_summary":
		label := strings.TrimSpace(artifactStringValue(summary["dominant_label"]))
		count := usageIntValue(summary["dominant_label_count"])
		if label != "" {
			return fmt.Sprintf("지배적인 감성은 %s이며 %d건입니다.", label, count)
		}
	case "issue_breakdown_summary":
		topGroup := strings.TrimSpace(artifactStringValue(summary["top_group"]))
		count := usageIntValue(summary["top_group_count"])
		if topGroup != "" {
			return fmt.Sprintf("최다 그룹은 %s이며 %d건입니다.", topGroup, count)
		}
	case "issue_trend_summary":
		peak := strings.TrimSpace(artifactStringValue(summary["peak_bucket"]))
		count := usageIntValue(summary["peak_count"])
		if peak != "" {
			return fmt.Sprintf("피크 구간은 %s이며 %d건입니다.", peak, count)
		}
	case "issue_period_compare":
		currentCount := usageIntValue(summary["current_count"])
		previousCount := usageIntValue(summary["previous_count"])
		countDelta := usageIntValue(summary["count_delta"])
		return fmt.Sprintf("현재 기간 %d건, 이전 기간 %d건으로 %d건 변화했습니다.", currentCount, previousCount, countDelta)
	case "structured_kpi_summary":
		rowCount := usageIntValue(summary["row_count"])
		metricSum := usageFloatValue(summary["metric_sum"])
		metricAvg := usageFloatValue(summary["metric_avg"])
		return fmt.Sprintf("구조화 KPI %d행을 집계했고 합계 %.2f, 평균 %.2f입니다.", rowCount, metricSum, metricAvg)
	case "semantic_search":
		matches, _ := artifact["matches"].([]any)
		if len(matches) > 0 {
			first, _ := matches[0].(map[string]any)
			text := strings.TrimSpace(artifactStringValue(first["text"]))
			if text != "" {
				return fmt.Sprintf("가장 관련 높은 근거는 '%s' 입니다.", text)
			}
		}
	case "document_filter":
		count := usageIntValue(summary["filtered_row_count"])
		inputCount := usageIntValue(summary["input_row_count"])
		return fmt.Sprintf("%d개 행을 선택했습니다. 전체 입력은 %d개였습니다.", count, inputCount)
	case "deduplicate_documents":
		canonicalCount := usageIntValue(summary["canonical_row_count"])
		duplicateCount := usageIntValue(summary["duplicate_row_count"])
		return fmt.Sprintf("중복 제거 후 대표 행은 %d개이며 중복 행은 %d개였습니다.", canonicalCount, duplicateCount)
	case "garbage_filter":
		removedCount := usageIntValue(summary["removed_row_count"])
		retainedCount := usageIntValue(summary["retained_row_count"])
		return fmt.Sprintf("가비지 문서 %d건을 제거하고 %d건을 유지했습니다.", removedCount, retainedCount)
	}
	if findings := executionArtifactKeyFindings(artifact); len(findings) > 0 {
		return findings[0]
	}
	return ""
}

func executionArtifactKeyFindings(artifact map[string]any) []string {
	findings := executionArtifactStringList(artifact["key_findings"])
	if len(findings) > 0 {
		return findings
	}
	return deriveExecutionFindings(artifact)
}

func deriveExecutionFindings(artifact map[string]any) []string {
	skillName := strings.TrimSpace(artifactStringValue(artifact["skill_name"]))
	summary, _ := artifact["summary"].(map[string]any)
	findings := make([]string, 0)
	switch skillName {
	case "issue_cluster_summary":
		for _, item := range executionArtifactMapSlice(artifact["clusters"], 3) {
			label := strings.TrimSpace(artifactStringValue(item["label"]))
			count := usageIntValue(item["document_count"])
			if label != "" && count > 0 {
				findings = append(findings, fmt.Sprintf("%s 군집이 %d건입니다.", label, count))
			}
		}
	case "issue_taxonomy_summary":
		for _, item := range executionArtifactMapSlice(artifact["taxonomy_breakdown"], 3) {
			label := strings.TrimSpace(artifactStringValue(item["label"]))
			if label == "" {
				label = strings.TrimSpace(artifactStringValue(item["taxonomy_id"]))
			}
			count := usageIntValue(item["count"])
			if label != "" && count > 0 {
				findings = append(findings, fmt.Sprintf("%s taxonomy가 %d건입니다.", label, count))
			}
		}
	case "issue_sentiment_summary":
		for _, item := range executionArtifactMapSlice(artifact["breakdown"], 3) {
			label := strings.TrimSpace(artifactStringValue(item["sentiment_label"]))
			count := usageIntValue(item["count"])
			if label != "" && count > 0 {
				findings = append(findings, fmt.Sprintf("%s 감성이 %d건입니다.", label, count))
			}
		}
	case "issue_breakdown_summary":
		for _, item := range executionArtifactMapSlice(artifact["breakdown"], 3) {
			group := strings.TrimSpace(artifactStringValue(item["group"]))
			count := usageIntValue(item["count"])
			if group != "" && count > 0 {
				findings = append(findings, fmt.Sprintf("%s 그룹이 %d건입니다.", group, count))
			}
		}
	case "issue_trend_summary":
		if peak := strings.TrimSpace(artifactStringValue(summary["peak_bucket"])); peak != "" {
			findings = append(findings, fmt.Sprintf("피크 구간은 %s입니다.", peak))
		}
	case "issue_period_compare":
		currentCount := usageIntValue(summary["current_count"])
		previousCount := usageIntValue(summary["previous_count"])
		countDelta := usageIntValue(summary["count_delta"])
		findings = append(findings, fmt.Sprintf("현재 %d건, 이전 %d건, 변화량 %d건입니다.", currentCount, previousCount, countDelta))
	case "structured_kpi_summary":
		rowCount := usageIntValue(summary["row_count"])
		findings = append(findings, fmt.Sprintf("집계 대상은 %d행입니다.", rowCount))
	}
	return uniqueNonEmptyStrings(findings)
}

func executionArtifactEvidence(artifact map[string]any) []map[string]any {
	items := executionArtifactMapSlice(artifact["evidence"], 5)
	if len(items) > 0 {
		return items
	}
	skillName := strings.TrimSpace(artifactStringValue(artifact["skill_name"]))
	switch skillName {
	case "issue_cluster_summary":
		clusters := executionArtifactMapSlice(artifact["clusters"], 1)
		if len(clusters) == 0 {
			return nil
		}
		return executionArtifactMapSlice(clusters[0]["samples"], 3)
	case "issue_sentiment_summary":
		breakdown := executionArtifactMapSlice(artifact["breakdown"], 1)
		if len(breakdown) == 0 {
			return nil
		}
		samples := executionArtifactStringList(breakdown[0]["samples"])
		evidence := make([]map[string]any, 0, len(samples))
		for index, sample := range samples {
			evidence = append(evidence, map[string]any{
				"rank":    index + 1,
				"snippet": sample,
			})
		}
		return evidence
	default:
		return nil
	}
}

func executionUsageMap(artifact map[string]any) map[string]any {
	usage, ok := artifact["usage"].(map[string]any)
	if !ok || len(usage) == 0 {
		return nil
	}
	return usage
}

func firstArtifactRef(artifact map[string]any) string {
	for _, key := range []string{"artifact_ref", "chunk_ref", "embedding_index_ref"} {
		if value := strings.TrimSpace(artifactStringValue(artifact[key])); value != "" {
			return value
		}
	}
	return ""
}

func artifactWarnings(artifact map[string]any) []string {
	warnings := make([]string, 0)
	for _, note := range executionArtifactStringList(artifact["notes"]) {
		lower := strings.ToLower(note)
		if strings.Contains(lower, "fallback") || strings.Contains(lower, "warning") || strings.Contains(lower, "error") || strings.Contains(lower, "failed") {
			warnings = append(warnings, note)
		}
	}
	return warnings
}

func executionArtifactMapSlice(value any, limit int) []map[string]any {
	rawItems, ok := value.([]any)
	if !ok {
		if typed, ok := value.([]map[string]any); ok {
			if limit > 0 && len(typed) > limit {
				return typed[:limit]
			}
			return typed
		}
		return nil
	}
	items := make([]map[string]any, 0, len(rawItems))
	for _, raw := range rawItems {
		item, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		items = append(items, item)
		if limit > 0 && len(items) >= limit {
			break
		}
	}
	return items
}

func executionArtifactStringList(value any) []string {
	switch typed := value.(type) {
	case []string:
		return uniqueNonEmptyStrings(typed)
	case []any:
		items := make([]string, 0, len(typed))
		for _, entry := range typed {
			text := strings.TrimSpace(artifactStringValue(entry))
			if text != "" {
				items = append(items, text)
			}
		}
		return uniqueNonEmptyStrings(items)
	default:
		return nil
	}
}

func sortedArtifactKeysFromDecoded(decoded map[string]map[string]any) []string {
	keys := make([]string, 0, len(decoded))
	for key := range decoded {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func artifactKeyForStep(keys []string, stepID, skillName string) string {
	prefix := "step:" + strings.TrimSpace(stepID) + ":" + strings.TrimSpace(skillName)
	for _, key := range keys {
		if key == prefix {
			return key
		}
	}
	return ""
}

func selectPrimaryBySkills(decoded map[string]map[string]any, skillNames ...string) (string, map[string]any) {
	keys := sortedArtifactKeysFromDecoded(decoded)
	for _, skillName := range skillNames {
		for _, key := range keys {
			if strings.HasSuffix(key, ":"+skillName) {
				return key, decoded[key]
			}
		}
	}
	return "", nil
}

func uniqueNonEmptyStrings(items []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func usageRoundCost(value float64) float64 {
	return float64(int(value*100000000+0.5)) / 100000000
}

func artifactStringValue(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func (s *AnalysisService) ResumeExecution(projectID, executionID string, input domain.ExecutionResumeRequest) (domain.ExecutionSummary, error) {
	execution, err := s.GetExecution(projectID, executionID)
	if err != nil {
		return domain.ExecutionSummary{}, err
	}
	if execution.Status != "waiting" {
		return domain.ExecutionSummary{}, ErrInvalidArgument{Message: "only waiting executions can be resumed"}
	}
	reason := "external dependency is ready"
	if input.Reason != nil && strings.TrimSpace(*input.Reason) != "" {
		reason = strings.TrimSpace(*input.Reason)
	}
	triggeredBy := "manual"
	if input.TriggeredBy != nil && strings.TrimSpace(*input.TriggeredBy) != "" {
		triggeredBy = strings.TrimSpace(*input.TriggeredBy)
	}
	return s.resumeExecutionInternal(execution, reason, triggeredBy)
}

func (s *AnalysisService) ResumeWaitingExecutionsForDatasetVersion(projectID, datasetVersionID, reason, triggeredBy string) (int, error) {
	versionID := strings.TrimSpace(datasetVersionID)
	if versionID == "" {
		return 0, nil
	}
	executions, err := s.store.ListExecutions(projectID)
	if err != nil {
		return 0, err
	}
	resumedCount := 0
	for _, item := range executions {
		if item.Status != "waiting" || item.DatasetVersionID == nil || strings.TrimSpace(*item.DatasetVersionID) != versionID {
			continue
		}
		execution, err := s.GetExecution(projectID, item.ExecutionID)
		if err != nil {
			return resumedCount, err
		}
		if execution.Status != "waiting" {
			continue
		}
		version, err := s.store.GetDatasetVersion(projectID, versionID)
		if err != nil {
			if err == store.ErrNotFound {
				return resumedCount, ErrNotFound{Resource: "dataset version"}
			}
			return resumedCount, err
		}
		version, err = s.ensureExecutionDependenciesForVersion(projectID, version, execution.Plan, triggeredBy)
		if err != nil {
			return resumedCount, err
		}
		latest, err := s.store.GetDatasetVersion(projectID, versionID)
		if err == nil {
			version = latest
		}
		if !planDependenciesReady(execution.Plan, version) {
			continue
		}
		if _, err := s.resumeExecutionInternal(execution, reason, triggeredBy); err != nil {
			return resumedCount, err
		}
		resumedCount++
	}
	return resumedCount, nil
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
		ProfileSnapshot:    cloneDatasetProfile(execution.ProfileSnapshot),
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
	skills := registry.DefaultPlanSkills(dataType)

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

func planRequiresPrepare(plan domain.SkillPlan) bool {
	for _, step := range plan.Steps {
		definition, ok := registry.Skill(step.SkillName)
		if ok && definition.RequiresPrepare {
			return true
		}
	}
	return false
}

func planRequiresSentiment(plan domain.SkillPlan) bool {
	for _, step := range plan.Steps {
		definition, ok := registry.Skill(step.SkillName)
		if ok && definition.RequiresSentiment {
			return true
		}
	}
	return false
}

func planRequiresEmbedding(plan domain.SkillPlan) bool {
	for _, step := range plan.Steps {
		definition, ok := registry.Skill(step.SkillName)
		if ok && definition.RequiresEmbedding {
			return true
		}
	}
	return false
}

func planRequiresCluster(plan domain.SkillPlan) bool {
	for _, step := range plan.Steps {
		if strings.TrimSpace(step.SkillName) == "embedding_cluster" {
			return true
		}
	}
	return false
}

func datasetPrepareReady(version domain.DatasetVersion) bool {
	return version.PrepareStatus == "ready" && version.PrepareURI != nil && strings.TrimSpace(*version.PrepareURI) != ""
}

func datasetSentimentReady(version domain.DatasetVersion) bool {
	return version.SentimentStatus == "ready" && version.SentimentURI != nil && strings.TrimSpace(*version.SentimentURI) != ""
}

func datasetEmbeddingReady(version domain.DatasetVersion) bool {
	if version.EmbeddingStatus != "ready" {
		return false
	}
	if version.EmbeddingURI != nil && strings.TrimSpace(*version.EmbeddingURI) != "" {
		return true
	}
	if strings.TrimSpace(metadataString(version.Metadata, "embedding_index_source_ref", "")) != "" {
		return true
	}
	if strings.TrimSpace(metadataString(version.Metadata, "embedding_index_ref", "")) != "" {
		return true
	}
	return false
}

func planDependenciesReady(plan domain.SkillPlan, version domain.DatasetVersion) bool {
	if planRequiresPrepare(plan) && requiresPrepare(version) && !datasetPrepareReady(version) {
		return false
	}
	if planRequiresSentiment(plan) && !datasetSentimentReady(version) {
		return false
	}
	if planRequiresEmbedding(plan) && !datasetEmbeddingReady(version) {
		return false
	}
	if planRequiresCluster(plan) && !clusterPlanReady(plan, version) {
		return false
	}
	return true
}

func (s *AnalysisService) resumeExecutionInternal(execution domain.ExecutionSummary, reason, triggeredBy string) (domain.ExecutionSummary, error) {
	if execution.DatasetVersionID != nil && strings.TrimSpace(*execution.DatasetVersionID) != "" {
		version, err := s.store.GetDatasetVersion(execution.ProjectID, strings.TrimSpace(*execution.DatasetVersionID))
		if err != nil && err != store.ErrNotFound {
			return domain.ExecutionSummary{}, err
		}
		if err == nil {
			execution.Plan = refreshPlanWithDatasetVersion(execution.Plan, version)
		}
	}
	now := time.Now().UTC()
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

func refreshPlanWithDatasetVersion(plan domain.SkillPlan, version domain.DatasetVersion) domain.SkillPlan {
	fallback := strings.TrimSpace(version.StorageURI)
	for index := range plan.Steps {
		definition, ok := registry.Skill(plan.Steps[index].SkillName)
		if !ok {
			continue
		}
		switch definition.DatasetSource {
		case "prepared", "sentiment":
			plan.Steps[index].DatasetName = resolvedDatasetNameForSkill(plan.Steps[index].SkillName, fallback, version)
		}
		if plan.Steps[index].Inputs == nil {
			plan.Steps[index].Inputs = map[string]any{}
		}
		for key, metadataKey := range definition.MetadataDefaults {
			current := plan.Steps[index].Inputs[key]
			plan.Steps[index].Inputs[key] = metadataValue(version.Metadata, metadataKey, current)
		}
		if _, hasTextColumn := definition.DefaultInputs["text_column"]; hasTextColumn {
			plan.Steps[index].Inputs["text_column"] = resolvedTextColumnForSkill(plan.Steps[index].Inputs, version)
		}
		if definition.RequiresEmbedding {
			if value := strings.TrimSpace(metadataString(version.Metadata, "embedding_index_ref", "")); value != "" {
				plan.Steps[index].Inputs["embedding_index_ref"] = value
			}
			if value := strings.TrimSpace(metadataString(version.Metadata, "chunk_ref", "")); value != "" {
				plan.Steps[index].Inputs["chunk_ref"] = value
			}
			if value := strings.TrimSpace(metadataString(version.Metadata, "chunk_format", "")); value != "" {
				plan.Steps[index].Inputs["chunk_format"] = value
			}
			if version.EmbeddingURI != nil && strings.TrimSpace(*version.EmbeddingURI) != "" {
				plan.Steps[index].Inputs["embedding_uri"] = strings.TrimSpace(*version.EmbeddingURI)
			} else {
				delete(plan.Steps[index].Inputs, "embedding_uri")
			}
		}
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
	definition, ok := registry.Skill(skillName)
	if !ok {
		return map[string]any{}
	}
	inputs := cloneInputMap(definition.DefaultInputs)
	if definition.GoalInput != "" && strings.TrimSpace(goal) != "" {
		inputs[definition.GoalInput] = strings.TrimSpace(goal)
	}
	return inputs
}

func enrichInputsForSkill(step *domain.SkillPlanStep, version domain.DatasetVersion, goal string) {
	if step.Inputs == nil {
		step.Inputs = map[string]any{}
	}
	explicitGarbageRules := inputPresent(step.Inputs, "garbage_rule_names")
	definition, ok := registry.Skill(step.SkillName)
	if !ok {
		return
	}
	defaults := defaultInputsForSkill(step.SkillName, goal)
	for key, defaultValue := range defaults {
		if inputPresent(step.Inputs, key) {
			continue
		}
		if metadataKey, hasMetadataOverride := definition.MetadataDefaults[key]; hasMetadataOverride {
			step.Inputs[key] = metadataValue(version.Metadata, metadataKey, defaultValue)
			continue
		}
		step.Inputs[key] = defaultValue
	}
	if _, hasTextColumn := defaults["text_column"]; hasTextColumn {
		step.Inputs["text_column"] = resolvedTextColumnForSkill(step.Inputs, version)
	}
	if definition.GoalInput != "" && !inputPresent(step.Inputs, definition.GoalInput) && strings.TrimSpace(goal) != "" {
		step.Inputs[definition.GoalInput] = strings.TrimSpace(goal)
	}
	if step.SkillName == "garbage_filter" && !explicitGarbageRules && version.Profile != nil && len(version.Profile.GarbageRuleNames) > 0 {
		step.Inputs["garbage_rule_names"] = append([]string(nil), version.Profile.GarbageRuleNames...)
	}
	if definition.RequiresEmbedding && !inputPresent(step.Inputs, "embedding_uri") && !inputPresent(step.Inputs, "embedding_index_ref") {
		if value := strings.TrimSpace(metadataString(version.Metadata, "embedding_index_ref", "")); value != "" {
			step.Inputs["embedding_index_ref"] = value
		}
		if value := strings.TrimSpace(metadataString(version.Metadata, "chunk_ref", "")); value != "" {
			step.Inputs["chunk_ref"] = value
		}
		if value := strings.TrimSpace(metadataString(version.Metadata, "chunk_format", "")); value != "" {
			step.Inputs["chunk_format"] = value
		}
		if !inputPresent(step.Inputs, "embedding_index_ref") {
			step.Inputs["embedding_uri"] = deriveEmbeddingURI(version)
		}
	}
	if step.SkillName == "embedding_cluster" && !inputPresent(step.Inputs, "cluster_ref") && clusterStepReady(*step, version) {
		if value := strings.TrimSpace(metadataString(version.Metadata, "cluster_ref", "")); value != "" {
			step.Inputs["cluster_ref"] = value
		}
		if value := strings.TrimSpace(metadataString(version.Metadata, "cluster_format", "")); value != "" {
			step.Inputs["cluster_format"] = value
		}
	}
}

func resolvedDatasetNameForSkill(skillName, fallback string, version domain.DatasetVersion) string {
	definition, ok := registry.Skill(skillName)
	if !ok {
		return fallback
	}
	switch definition.DatasetSource {
	case "sentiment":
		return datasetSourceForSentiment(version)
	case "prepared":
		return datasetSourceForUnstructured(version)
	default:
		return fallback
	}
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
	if text == "" || text == rawTextColumn || (text == "text" && rawTextColumn != "text") {
		return defaultTextColumn
	}
	return text
}

func cloneInputMap(source map[string]any) map[string]any {
	cloned := make(map[string]any, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func inputPresent(inputs map[string]any, key string) bool {
	if inputs == nil {
		return false
	}
	value, ok := inputs[key]
	if !ok || value == nil {
		return false
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value)) != ""
}

func metadataValue(metadata map[string]any, key string, fallback any) any {
	if metadata == nil {
		return fallback
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return fallback
	}
	if strings.TrimSpace(fmt.Sprintf("%v", value)) == "" {
		return fallback
	}
	return value
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

func stringPointer(value string) *string {
	return &value
}
