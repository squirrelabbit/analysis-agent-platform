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
	if scenario.PlanningMode != scenarioPlanningModeStrict {
		t.Fatalf("unexpected planning mode: %+v", scenario)
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

func TestCreateScenarioRejectsUnsupportedPlanningMode(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewScenarioService(repository)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	mode := "guided"
	_, err := service.CreateScenario(project.ProjectID, domain.ScenarioCreateRequest{
		ScenarioID:     "S3",
		PlanningMode:   &mode,
		UserQuery:      "이번 벚꽃 축제 반응 어때?",
		QueryType:      "여론 요약",
		Interpretation: "전체 여론 및 분위기 파악",
		AnalysisScope:  "축제 기간",
		Steps: []domain.ScenarioStep{
			{
				Step:              1,
				FunctionName:      "가비지 필터링",
				ResultDescription: "분석 대상 정제",
			},
		},
	})
	if err == nil {
		t.Fatal("expected unsupported planning mode error")
	}
	if err.Error() != `planning_mode "guided" is not supported yet; use "strict"` {
		t.Fatalf("unexpected error: %v", err)
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
	if scenarioContext["planning_mode"] != scenarioPlanningModeStrict {
		t.Fatalf("unexpected scenario planning mode: %+v", scenarioContext)
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

func TestImportScenariosGroupsRowsByScenarioID(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewScenarioService(repository)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	response, err := service.ImportScenarios(project.ProjectID, domain.ScenarioImportRequest{
		Rows: []domain.ScenarioImportRow{
			{
				ScenarioID:        "S1",
				UserQuery:         "이번 벚꽃 축제 반응 어때?",
				QueryType:         "여론 요약",
				Interpretation:    "전체 여론 및 분위기 파악",
				AnalysisScope:     "축제 기간",
				Step:              2,
				FunctionName:      "빈도 기반 키워드 추출",
				ParameterText:     stringPointer("top_n=10"),
				ResultDescription: "주요 키워드",
			},
			{
				ScenarioID:        "S1",
				UserQuery:         "이번 벚꽃 축제 반응 어때?",
				QueryType:         "여론 요약",
				Interpretation:    "전체 여론 및 분위기 파악",
				AnalysisScope:     "축제 기간",
				Step:              1,
				FunctionName:      "가비지 필터링",
				ResultDescription: "분석 대상 정제",
			},
			{
				ScenarioID:        "S2",
				UserQuery:         "이번 축제 문제 뭐였어?",
				QueryType:         "이슈 분석",
				Interpretation:    "부정 의견 중심 문제 파악",
				AnalysisScope:     "축제 기간",
				Step:              1,
				FunctionName:      "문서 단위 감성 분류",
				RuntimeSkillName:  stringPointer("issue_sentiment_summary"),
				ResultDescription: "감성 분류",
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected import scenarios error: %v", err)
	}
	if response.ScenarioCount != 2 || response.RowCount != 3 {
		t.Fatalf("unexpected import response: %+v", response)
	}
	if len(response.Items) != 2 {
		t.Fatalf("unexpected imported items: %+v", response.Items)
	}
	if response.Items[0].PlanningMode != scenarioPlanningModeStrict {
		t.Fatalf("expected default strict planning mode: %+v", response.Items[0])
	}
	if response.Items[0].Steps[0].Step != 1 || response.Items[0].Steps[1].Step != 2 {
		t.Fatalf("expected sorted steps: %+v", response.Items[0].Steps)
	}
}

func TestImportScenariosRejectsInconsistentHeaderValues(t *testing.T) {
	repository := store.NewMemoryStore()
	service := NewScenarioService(repository)

	project := domain.Project{ProjectID: "project-1", Name: "demo"}
	if err := repository.SaveProject(project); err != nil {
		t.Fatalf("unexpected save project error: %v", err)
	}

	_, err := service.ImportScenarios(project.ProjectID, domain.ScenarioImportRequest{
		Rows: []domain.ScenarioImportRow{
			{
				ScenarioID:        "S1",
				UserQuery:         "이번 벚꽃 축제 반응 어때?",
				QueryType:         "여론 요약",
				Interpretation:    "전체 여론 및 분위기 파악",
				AnalysisScope:     "축제 기간",
				Step:              1,
				FunctionName:      "가비지 필터링",
				ResultDescription: "분석 대상 정제",
			},
			{
				ScenarioID:        "S1",
				UserQuery:         "이번 벚꽃 축제 반응 어때?",
				QueryType:         "트렌드 분석",
				Interpretation:    "전체 여론 및 분위기 파악",
				AnalysisScope:     "축제 기간",
				Step:              2,
				FunctionName:      "빈도 기반 키워드 추출",
				ResultDescription: "주요 키워드",
			},
		},
	})
	if err == nil {
		t.Fatal("expected inconsistent header error")
	}
	if err.Error() != `scenario "S1" has inconsistent query_type values` {
		t.Fatalf("unexpected error: %v", err)
	}
}
