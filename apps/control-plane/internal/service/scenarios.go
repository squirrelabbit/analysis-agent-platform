package service

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/registry"
	"analysis-support-platform/control-plane/internal/store"
)

type ScenarioService struct {
	store store.Repository
}

const scenarioPlanningModeStrict = "strict"

type groupedScenario struct {
	planningMode   *string
	userQuery      string
	queryType      string
	interpretation string
	analysisScope  string
	steps          []domain.ScenarioStep
	seenSteps      map[int]struct{}
}

func NewScenarioService(repository store.Repository) *ScenarioService {
	return &ScenarioService{store: repository}
}

func (s *ScenarioService) CreateScenario(projectID string, input domain.ScenarioCreateRequest) (domain.Scenario, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.Scenario{}, ErrNotFound{Resource: "project"}
		}
		return domain.Scenario{}, err
	}
	if strings.TrimSpace(input.ScenarioID) == "" {
		return domain.Scenario{}, ErrInvalidArgument{Message: "scenario_id is required"}
	}
	if strings.TrimSpace(input.UserQuery) == "" {
		return domain.Scenario{}, ErrInvalidArgument{Message: "user_query is required"}
	}
	if strings.TrimSpace(input.QueryType) == "" {
		return domain.Scenario{}, ErrInvalidArgument{Message: "query_type is required"}
	}
	if strings.TrimSpace(input.Interpretation) == "" {
		return domain.Scenario{}, ErrInvalidArgument{Message: "interpretation is required"}
	}
	if strings.TrimSpace(input.AnalysisScope) == "" {
		return domain.Scenario{}, ErrInvalidArgument{Message: "analysis_scope is required"}
	}
	if len(input.Steps) == 0 {
		return domain.Scenario{}, ErrInvalidArgument{Message: "steps is required"}
	}
	planningMode, err := normalizeScenarioPlanningMode(input.PlanningMode)
	if err != nil {
		return domain.Scenario{}, err
	}

	normalizedSteps := make([]domain.ScenarioStep, 0, len(input.Steps))
	for _, step := range input.Steps {
		if step.Step <= 0 {
			return domain.Scenario{}, ErrInvalidArgument{Message: "each step must be greater than 0"}
		}
		if strings.TrimSpace(step.FunctionName) == "" {
			return domain.Scenario{}, ErrInvalidArgument{Message: "function_name is required"}
		}
		if strings.TrimSpace(step.ResultDescription) == "" {
			return domain.Scenario{}, ErrInvalidArgument{Message: "result_description is required"}
		}
		step.FunctionName = strings.TrimSpace(step.FunctionName)
		step.ResultDescription = strings.TrimSpace(step.ResultDescription)
		if step.RuntimeSkillName != nil {
			trimmed := strings.TrimSpace(*step.RuntimeSkillName)
			if trimmed == "" {
				step.RuntimeSkillName = nil
			} else {
				step.RuntimeSkillName = &trimmed
			}
		}
		if step.ParameterText != nil {
			trimmed := strings.TrimSpace(*step.ParameterText)
			if trimmed == "" {
				step.ParameterText = nil
			} else {
				step.ParameterText = &trimmed
			}
		}
		if step.Parameters == nil {
			step.Parameters = map[string]any{}
		}
		normalizedSteps = append(normalizedSteps, step)
	}

	scenario := domain.Scenario{
		ScenarioID:     strings.TrimSpace(input.ScenarioID),
		ProjectID:      projectID,
		PlanningMode:   planningMode,
		UserQuery:      strings.TrimSpace(input.UserQuery),
		QueryType:      strings.TrimSpace(input.QueryType),
		Interpretation: strings.TrimSpace(input.Interpretation),
		AnalysisScope:  strings.TrimSpace(input.AnalysisScope),
		Steps:          normalizedSteps,
		CreatedAt:      time.Now().UTC(),
	}
	if existing, err := s.store.GetScenario(projectID, scenario.ScenarioID); err == nil {
		scenario.CreatedAt = existing.CreatedAt
	}
	if err := s.store.SaveScenario(scenario); err != nil {
		return domain.Scenario{}, err
	}
	return scenario, nil
}

func (s *ScenarioService) GetScenario(projectID, scenarioID string) (domain.Scenario, error) {
	scenario, err := s.store.GetScenario(projectID, scenarioID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Scenario{}, ErrNotFound{Resource: "scenario"}
		}
		return domain.Scenario{}, err
	}
	return scenario, nil
}

func (s *ScenarioService) ListScenarios(projectID string) (domain.ScenarioListResponse, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ScenarioListResponse{}, ErrNotFound{Resource: "project"}
		}
		return domain.ScenarioListResponse{}, err
	}
	scenarios, err := s.store.ListScenarios(projectID)
	if err != nil {
		return domain.ScenarioListResponse{}, err
	}
	return domain.ScenarioListResponse{Items: scenarios}, nil
}

func (s *ScenarioService) ImportScenarios(projectID string, input domain.ScenarioImportRequest) (domain.ScenarioImportResponse, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.ScenarioImportResponse{}, ErrNotFound{Resource: "project"}
		}
		return domain.ScenarioImportResponse{}, err
	}
	if len(input.Rows) == 0 {
		return domain.ScenarioImportResponse{}, ErrInvalidArgument{Message: "rows is required"}
	}

	grouped := make(map[string]*groupedScenario)
	order := make([]string, 0)
	for _, row := range input.Rows {
		scenarioID := strings.TrimSpace(row.ScenarioID)
		if scenarioID == "" {
			return domain.ScenarioImportResponse{}, ErrInvalidArgument{Message: "rows[].scenario_id is required"}
		}
		if _, exists := grouped[scenarioID]; !exists {
			grouped[scenarioID] = &groupedScenario{
				planningMode:   row.PlanningMode,
				userQuery:      strings.TrimSpace(row.UserQuery),
				queryType:      strings.TrimSpace(row.QueryType),
				interpretation: strings.TrimSpace(row.Interpretation),
				analysisScope:  strings.TrimSpace(row.AnalysisScope),
				steps:          []domain.ScenarioStep{},
				seenSteps:      map[int]struct{}{},
			}
			order = append(order, scenarioID)
		}

		group := grouped[scenarioID]
		if err := validateScenarioImportHeader(row, group); err != nil {
			return domain.ScenarioImportResponse{}, err
		}
		if row.Step <= 0 {
			return domain.ScenarioImportResponse{}, ErrInvalidArgument{
				Message: fmt.Sprintf("scenario %q step must be greater than 0", scenarioID),
			}
		}
		if _, exists := group.seenSteps[row.Step]; exists {
			return domain.ScenarioImportResponse{}, ErrInvalidArgument{
				Message: fmt.Sprintf("scenario %q has duplicated step %d", scenarioID, row.Step),
			}
		}
		group.seenSteps[row.Step] = struct{}{}
		group.steps = append(group.steps, domain.ScenarioStep{
			Step:              row.Step,
			FunctionName:      row.FunctionName,
			RuntimeSkillName:  row.RuntimeSkillName,
			ParameterText:     row.ParameterText,
			Parameters:        row.Parameters,
			ResultDescription: row.ResultDescription,
		})
	}

	items := make([]domain.Scenario, 0, len(order))
	for _, scenarioID := range order {
		group := grouped[scenarioID]
		sort.SliceStable(group.steps, func(i, j int) bool {
			return group.steps[i].Step < group.steps[j].Step
		})
		scenario, err := s.CreateScenario(projectID, domain.ScenarioCreateRequest{
			ScenarioID:     scenarioID,
			PlanningMode:   group.planningMode,
			UserQuery:      group.userQuery,
			QueryType:      group.queryType,
			Interpretation: group.interpretation,
			AnalysisScope:  group.analysisScope,
			Steps:          group.steps,
		})
		if err != nil {
			return domain.ScenarioImportResponse{}, err
		}
		items = append(items, scenario)
	}

	return domain.ScenarioImportResponse{
		ScenarioCount: len(items),
		RowCount:      len(input.Rows),
		Items:         items,
	}, nil
}

func (s *ScenarioService) BuildAnalysisSubmitRequest(projectID, scenarioID string, input domain.ScenarioPlanCreateRequest) (domain.AnalysisSubmitRequest, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.AnalysisSubmitRequest{}, ErrNotFound{Resource: "project"}
		}
		return domain.AnalysisSubmitRequest{}, err
	}

	scenario, err := s.GetScenario(projectID, scenarioID)
	if err != nil {
		return domain.AnalysisSubmitRequest{}, err
	}

	datasetID := strings.TrimSpace(input.DatasetID)
	datasetVersionID := strings.TrimSpace(input.DatasetVersionID)
	if datasetID == "" && datasetVersionID == "" {
		return domain.AnalysisSubmitRequest{}, ErrInvalidArgument{Message: "dataset_id is required"}
	}
	if scenario.PlanningMode != scenarioPlanningModeStrict {
		return domain.AnalysisSubmitRequest{}, ErrInvalidArgument{
			Message: fmt.Sprintf("scenario planning_mode %q is not supported yet; only %q is available", scenario.PlanningMode, scenarioPlanningModeStrict),
		}
	}

	goal := scenario.UserQuery
	if input.Goal != nil && strings.TrimSpace(*input.Goal) != "" {
		goal = strings.TrimSpace(*input.Goal)
	}

	plan, err := buildScenarioPlan(scenario, goal)
	if err != nil {
		return domain.AnalysisSubmitRequest{}, err
	}

	context := cloneInputMap(input.Context)
	if context == nil {
		context = map[string]any{}
	}
	context["scenario"] = map[string]any{
		"scenario_id":           scenario.ScenarioID,
		"planning_mode":         scenario.PlanningMode,
		"user_query":            scenario.UserQuery,
		"query_type":            scenario.QueryType,
		"interpretation":        scenario.Interpretation,
		"analysis_scope":        scenario.AnalysisScope,
		"registered_step_count": len(scenario.Steps),
	}

	constraints := append([]string(nil), input.Constraints...)
	submitRequest := domain.AnalysisSubmitRequest{
		Goal:          goal,
		Constraints:   constraints,
		Context:       context,
		RequestedPlan: &plan,
	}
	if datasetID != "" {
		submitRequest.DatasetID = &datasetID
	} else {
		submitRequest.DatasetVersionID = &datasetVersionID
	}
	return submitRequest, nil
}

func buildScenarioPlan(scenario domain.Scenario, goal string) (domain.SkillPlan, error) {
	orderedSteps := append([]domain.ScenarioStep(nil), scenario.Steps...)
	sort.SliceStable(orderedSteps, func(i, j int) bool {
		return orderedSteps[i].Step < orderedSteps[j].Step
	})

	planSteps := make([]domain.SkillPlanStep, 0, len(orderedSteps))
	for _, step := range orderedSteps {
		skillName, err := resolveScenarioSkillName(step)
		if err != nil {
			return domain.SkillPlan{}, err
		}
		inputs := defaultInputsForSkill(skillName, goal)
		for key, value := range scenarioStepParameters(step, skillName) {
			inputs[key] = value
		}
		planSteps = append(planSteps, domain.SkillPlanStep{
			StepID:      id.New(),
			SkillName:   skillName,
			DatasetName: "dataset_from_version",
			Inputs:      inputs,
		})
	}

	notes := fmt.Sprintf("generated from scenario %s (%s)", scenario.ScenarioID, scenario.QueryType)
	return domain.SkillPlan{
		PlanID:    id.New(),
		Steps:     planSteps,
		Notes:     &notes,
		CreatedAt: time.Now().UTC(),
	}, nil
}

func resolveScenarioSkillName(step domain.ScenarioStep) (string, error) {
	if step.RuntimeSkillName != nil && strings.TrimSpace(*step.RuntimeSkillName) != "" {
		skillName := strings.TrimSpace(*step.RuntimeSkillName)
		definition, ok := registry.Skill(skillName)
		if !ok {
			return "", ErrInvalidArgument{Message: fmt.Sprintf("scenario step %d runtime_skill_name %q is not registered", step.Step, skillName)}
		}
		if !definition.PlanEnabled {
			return "", ErrInvalidArgument{Message: fmt.Sprintf("scenario step %d runtime_skill_name %q is not plan-enabled", step.Step, skillName)}
		}
		return skillName, nil
	}

	skillName, ok := scenarioFunctionSkillAliases[strings.TrimSpace(step.FunctionName)]
	if !ok {
		return "", ErrInvalidArgument{
			Message: fmt.Sprintf("scenario step %d function_name %q cannot be mapped automatically; set runtime_skill_name explicitly", step.Step, step.FunctionName),
		}
	}
	return skillName, nil
}

func scenarioStepParameters(step domain.ScenarioStep, skillName string) map[string]any {
	parameters := cloneInputMap(step.Parameters)
	if len(parameters) == 0 && step.ParameterText != nil {
		parameters = parseScenarioParameterText(*step.ParameterText)
	}
	if parameters == nil {
		parameters = map[string]any{}
	}
	return normalizeScenarioSkillInputs(skillName, parameters)
}

func parseScenarioParameterText(text string) map[string]any {
	parsed := map[string]any{}
	for _, rawPart := range strings.Split(text, ",") {
		part := strings.TrimSpace(rawPart)
		if part == "" {
			continue
		}
		key, value, found := strings.Cut(part, "=")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		parsed[key] = parseScenarioScalar(value)
	}
	return parsed
}

func parseScenarioScalar(raw string) any {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if intValue, err := strconv.Atoi(trimmed); err == nil {
		return intValue
	}
	if floatValue, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return floatValue
	}
	lowered := strings.ToLower(trimmed)
	if lowered == "true" || lowered == "false" {
		return lowered == "true"
	}
	return trimmed
}

func normalizeScenarioSkillInputs(skillName string, inputs map[string]any) map[string]any {
	normalized := cloneInputMap(inputs)
	if normalized == nil {
		return map[string]any{}
	}

	if usesTimeBucket(skillName) {
		if _, exists := normalized["bucket"]; !exists {
			for _, alias := range []string{"period", "기간", "기간단위"} {
				if value, ok := normalized[alias]; ok {
					normalized["bucket"] = normalizeScenarioBucket(value)
					break
				}
			}
		}
	}

	return normalized
}

func usesTimeBucket(skillName string) bool {
	switch skillName {
	case "time_bucket_count", "issue_trend_summary", "issue_period_compare":
		return true
	default:
		return false
	}
}

func normalizeScenarioBucket(value any) any {
	trimmed := strings.TrimSpace(fmt.Sprint(value))
	switch trimmed {
	case "일", "day", "daily":
		return "day"
	case "주", "week", "weekly":
		return "week"
	case "월", "month", "monthly":
		return "month"
	default:
		return value
	}
}

var scenarioFunctionSkillAliases = map[string]string{
	"가비지 필터링":      "garbage_filter",
	"광고/가비지 제거":    "garbage_filter",
	"빈도 기반 키워드 추출": "keyword_frequency",
	"키워드 추출":       "keyword_frequency",
	"명사 기반 키워드 추출": "noun_frequency",
	"명사 빈도 추출":     "noun_frequency",
	"문장 분리":        "sentence_split",
	"기간별 문서량 추이":   "time_bucket_count",
	"문서량 시계열":      "time_bucket_count",
	"기간 구간 문서량 비교": "issue_period_compare",
	"감성 비율 집계":     "issue_sentiment_summary",
	"문서 단위 감성 분류":  "issue_sentiment_summary",
	"전체 담론 요약":     "issue_evidence_summary",
	"기간별 담론 요약":    "issue_trend_summary",
}

func normalizeScenarioPlanningMode(raw *string) (string, error) {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return scenarioPlanningModeStrict, nil
	}
	mode := strings.ToLower(strings.TrimSpace(*raw))
	if mode != scenarioPlanningModeStrict {
		return "", ErrInvalidArgument{
			Message: fmt.Sprintf("planning_mode %q is not supported yet; use %q", mode, scenarioPlanningModeStrict),
		}
	}
	return mode, nil
}

func validateScenarioImportHeader(row domain.ScenarioImportRow, group *groupedScenario) error {
	if group == nil {
		return nil
	}
	if strings.TrimSpace(row.UserQuery) != group.userQuery {
		return ErrInvalidArgument{Message: fmt.Sprintf("scenario %q has inconsistent user_query values", strings.TrimSpace(row.ScenarioID))}
	}
	if strings.TrimSpace(row.QueryType) != group.queryType {
		return ErrInvalidArgument{Message: fmt.Sprintf("scenario %q has inconsistent query_type values", strings.TrimSpace(row.ScenarioID))}
	}
	if strings.TrimSpace(row.Interpretation) != group.interpretation {
		return ErrInvalidArgument{Message: fmt.Sprintf("scenario %q has inconsistent interpretation values", strings.TrimSpace(row.ScenarioID))}
	}
	if strings.TrimSpace(row.AnalysisScope) != group.analysisScope {
		return ErrInvalidArgument{Message: fmt.Sprintf("scenario %q has inconsistent analysis_scope values", strings.TrimSpace(row.ScenarioID))}
	}
	currentMode := strings.TrimSpace(optionalImportMode(group.planningMode))
	incomingMode := strings.TrimSpace(optionalImportMode(row.PlanningMode))
	if currentMode == "" {
		group.planningMode = row.PlanningMode
		currentMode = strings.TrimSpace(optionalImportMode(group.planningMode))
	}
	if currentMode != "" && incomingMode != "" && currentMode != incomingMode {
		return ErrInvalidArgument{Message: fmt.Sprintf("scenario %q has inconsistent planning_mode values", strings.TrimSpace(row.ScenarioID))}
	}
	return nil
}

func optionalImportMode(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
