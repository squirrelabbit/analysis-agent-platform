package service

import (
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

func TestCreateScenarioStoresStructuredSteps(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewScenarioService(repository)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	runtimeSkill := "garbage_filter"
	parameterText := "top_n=10"
	scenario, err := service.CreateScenario(project.ProjectID, domain.ScenarioCreateRequest{
		ScenarioID:     "S1",
		UserQuery:      "이번 벚꽃 축제 반응 어때?",
		QueryType:      "여론 요약",
		Interpretation: "전체 여론 및 분위기 파악",
		AnalysisScope:  "축제 기간",
		Steps: []domain.ScenarioStep{
			{
				Step:              1,
				FunctionName:      "가비지 필터링",
				RuntimeSkillName:  &runtimeSkill,
				ResultDescription: "분석 대상 정제",
			},
			{
				Step:              2,
				FunctionName:      "빈도 기반 키워드 추출",
				ParameterText:     &parameterText,
				Parameters:        map[string]any{"top_n": 10},
				ResultDescription: "주요 키워드",
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected create scenario error: %v", err)
	}
	if scenario.ScenarioID != "S1" {
		t.Fatalf("unexpected scenario id: %+v", scenario)
	}
	if len(scenario.Steps) != 2 {
		t.Fatalf("unexpected scenario steps: %+v", scenario.Steps)
	}
	if scenario.Steps[1].Parameters["top_n"] != 10 {
		t.Fatalf("unexpected scenario parameters: %+v", scenario.Steps[1].Parameters)
	}

	listResponse, err := service.ListScenarios(project.ProjectID)
	if err != nil {
		t.Fatalf("unexpected list scenarios error: %v", err)
	}
	if len(listResponse.Items) != 1 {
		t.Fatalf("unexpected scenario list: %+v", listResponse.Items)
	}

	loaded, err := service.GetScenario(project.ProjectID, "S1")
	if err != nil {
		t.Fatalf("unexpected get scenario error: %v", err)
	}
	if loaded.UserQuery != "이번 벚꽃 축제 반응 어때?" {
		t.Fatalf("unexpected loaded scenario: %+v", loaded)
	}
}

func TestBuildScenarioAnalysisSubmitRequestMapsFunctionNames(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewScenarioService(repository)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	scenario, err := service.CreateScenario(project.ProjectID, domain.ScenarioCreateRequest{
		ScenarioID:     "S1",
		UserQuery:      "이번 벚꽃 축제 반응 어때?",
		QueryType:      "여론 요약",
		Interpretation: "전체 여론 및 분위기 파악",
		AnalysisScope:  "축제 기간",
		Steps: []domain.ScenarioStep{
			{
				Step:              2,
				FunctionName:      "빈도 기반 키워드 추출",
				ParameterText:     stringPointer("top_n=10"),
				ResultDescription: "주요 키워드",
			},
			{
				Step:              1,
				FunctionName:      "가비지 필터링",
				ResultDescription: "분석 대상 정제",
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected create scenario error: %v", err)
	}

	submitRequest, err := service.BuildAnalysisSubmitRequest(project.ProjectID, scenario.ScenarioID, domain.ScenarioPlanCreateRequest{
		DatasetVersionID: "version-1",
	})
	if err != nil {
		t.Fatalf("unexpected build analysis submit request error: %v", err)
	}
	if submitRequest.RequestedPlan == nil || len(submitRequest.RequestedPlan.Steps) != 2 {
		t.Fatalf("unexpected requested plan: %+v", submitRequest.RequestedPlan)
	}
	if submitRequest.Goal != scenario.UserQuery {
		t.Fatalf("unexpected goal: %+v", submitRequest)
	}
	if submitRequest.RequestedPlan.Steps[0].SkillName != "garbage_filter" {
		t.Fatalf("expected garbage_filter first, got %+v", submitRequest.RequestedPlan.Steps)
	}
	if submitRequest.RequestedPlan.Steps[1].SkillName != "keyword_frequency" {
		t.Fatalf("expected keyword_frequency second, got %+v", submitRequest.RequestedPlan.Steps)
	}
	if submitRequest.RequestedPlan.Steps[1].Inputs["top_n"] != 10 {
		t.Fatalf("expected parsed top_n input, got %+v", submitRequest.RequestedPlan.Steps[1].Inputs)
	}
	scenarioContext, ok := submitRequest.Context["scenario"].(map[string]any)
	if !ok {
		t.Fatalf("expected scenario context metadata: %+v", submitRequest.Context)
	}
	if scenarioContext["scenario_id"] != "S1" {
		t.Fatalf("unexpected scenario context: %+v", scenarioContext)
	}
}

func TestBuildScenarioAnalysisSubmitRequestRejectsUnsupportedFunctionName(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewScenarioService(repository)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	if _, err := service.CreateScenario(project.ProjectID, domain.ScenarioCreateRequest{
		ScenarioID:     "S2",
		UserQuery:      "언제 가장 많이 언급됐어?",
		QueryType:      "피크 분석",
		Interpretation: "최대 문서량 시점 파악",
		AnalysisScope:  "축제 기간",
		Steps: []domain.ScenarioStep{
			{
				Step:              1,
				FunctionName:      "피크 시점 탐지",
				ResultDescription: "최대 문서량 시점",
			},
		},
	}); err != nil {
		t.Fatalf("unexpected create scenario error: %v", err)
	}

	_, err := service.BuildAnalysisSubmitRequest(project.ProjectID, "S2", domain.ScenarioPlanCreateRequest{
		DatasetVersionID: "version-1",
	})
	if err == nil {
		t.Fatal("expected unsupported function_name error")
	}
	if err.Error() != `scenario step 1 function_name "피크 시점 탐지" cannot be mapped automatically; set runtime_skill_name explicitly` {
		t.Fatalf("unexpected error: %v", err)
	}
}
