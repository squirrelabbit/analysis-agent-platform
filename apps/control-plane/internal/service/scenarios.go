package service

import (
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

type ScenarioService struct {
	store store.Repository
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
