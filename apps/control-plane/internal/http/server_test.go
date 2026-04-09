package http

import (
	"bytes"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/config"
)

func TestControlPlaneFlow(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:                ":0",
		StoreBackend:            "memory",
		WorkflowEngine:          "noop",
		TemporalPersistenceMode: "dev_ephemeral",
		TemporalRetentionMode:   "temporal_dev_default",
		TemporalRecoveryMode:    "startup_reconciliation",
	})
	handler := server.Handler()

	runtimeStatus := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/runtime_status", "", http.StatusOK, &runtimeStatus)
	if runtimeStatus["status"] != "ok" || runtimeStatus["workflow_engine"] != "noop" {
		t.Fatalf("unexpected runtime status response: %+v", runtimeStatus)
	}

	project := map[string]any{}
	createProjectBody := `{"name":"demo-project","description":"test"}`
	readJSONResponse(t, handler, http.MethodPost, "/projects", createProjectBody, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets",
		`{"name":"sales-dataset","data_type":"structured"}`,
		http.StatusCreated,
		&dataset,
	)
	datasetID := dataset["dataset_id"].(string)

	version := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		`{"storage_uri":"sales_daily","data_type":"structured"}`,
		http.StatusCreated,
		&version,
	)
	versionID := version["dataset_version_id"].(string)

	analysis := map[string]any{}
	submitBody := `{
	  "dataset_version_id":"` + versionID + `",
	  "data_type":"structured",
	  "goal":"Why did sales drop?",
	  "constraints":["compare recent trend"],
	  "context":{"channel":"test"}
	}`
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/analysis_requests",
		submitBody,
		http.StatusCreated,
		&analysis,
	)
	plan := analysis["plan"].(map[string]any)
	planID := plan["plan_id"].(string)

	executionResponse := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/plans/"+planID+"/execute",
		"",
		http.StatusAccepted,
		&executionResponse,
	)
	execution := executionResponse["execution"].(map[string]any)
	executionID := execution["execution_id"].(string)

	result := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/executions/"+executionID+"/result",
		"",
		http.StatusOK,
		&result,
	)
	if result["execution_id"] != executionID {
		t.Fatalf("unexpected execution_id in result: %v", result["execution_id"])
	}
	resultV1, ok := result["result_v1"].(map[string]any)
	if !ok {
		t.Fatalf("expected result_v1 payload: %+v", result)
	}
	if resultV1["schema_version"] != "execution-result-v1" {
		t.Fatalf("unexpected result_v1 schema version: %+v", resultV1)
	}
	diagnostics, ok := result["diagnostics"].(map[string]any)
	if !ok {
		t.Fatalf("expected execution diagnostics: %+v", result)
	}
	if diagnostics["event_count"].(float64) < 1 {
		t.Fatalf("expected diagnostics event_count >= 1: %+v", diagnostics)
	}

	rerun := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/executions/"+executionID+"/rerun",
		`{"mode":"strict_repro","triggered_by":"test"}`,
		http.StatusAccepted,
		&rerun,
	)
	rerunExecution := rerun["execution"].(map[string]any)
	rerunExecutionID := rerunExecution["execution_id"].(string)

	diff := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/executions/diff?from="+executionID+"&to="+rerunExecutionID,
		"",
		http.StatusOK,
		&diff,
	)
	if diff["changed_steps"].(float64) != 0 {
		t.Fatalf("expected unchanged diff, got %v", diff["changed_steps"])
	}
}

func TestExecutionListAndReportDraftEndpoints(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"draft-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets",
		`{"name":"issues","data_type":"structured"}`,
		http.StatusCreated,
		&dataset,
	)
	datasetID := dataset["dataset_id"].(string)

	version := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		`{"storage_uri":"issues.csv","data_type":"structured"}`,
		http.StatusCreated,
		&version,
	)
	versionID := version["dataset_version_id"].(string)

	analysis := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/analysis_requests",
		`{"dataset_version_id":"`+versionID+`","data_type":"structured","goal":"이슈를 정리해줘"}`,
		http.StatusCreated,
		&analysis,
	)
	planID := analysis["plan"].(map[string]any)["plan_id"].(string)

	executionResponse := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/plans/"+planID+"/execute",
		"",
		http.StatusAccepted,
		&executionResponse,
	)
	executionID := executionResponse["execution"].(map[string]any)["execution_id"].(string)

	listResponse := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/executions",
		"",
		http.StatusOK,
		&listResponse,
	)
	items, ok := listResponse["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one execution list item: %+v", listResponse)
	}
	if diagnostics, ok := items[0].(map[string]any)["diagnostics"].(map[string]any); !ok || diagnostics["event_count"] == nil {
		t.Fatalf("expected execution diagnostics in list item: %+v", listResponse)
	}

	reportDraft := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/report_drafts",
		`{"title":"테스트 초안","execution_ids":["`+executionID+`"]}`,
		http.StatusCreated,
		&reportDraft,
	)
	draftID := reportDraft["draft_id"].(string)
	content := reportDraft["content"].(map[string]any)
	if content["schema_version"] != "report-draft-v1" {
		t.Fatalf("unexpected report draft schema: %+v", content)
	}

	loadedDraft := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/report_drafts/"+draftID,
		"",
		http.StatusOK,
		&loadedDraft,
	)
	if loadedDraft["draft_id"] != draftID {
		t.Fatalf("unexpected loaded draft: %+v", loadedDraft)
	}
}

func TestSkillPolicyCatalogEndpoints(t *testing.T) {
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/capabilities":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"prompt_catalog": []map[string]any{},
				"rule_catalog":   map[string]any{},
				"skill_policy_catalog": []map[string]any{
					{"version": "embedding-cluster-v1", "skill_name": "embedding_cluster", "policy_hash": "abc123"},
				},
				"skill_policy_validation": map[string]any{
					"valid": true,
					"catalog": []map[string]any{
						{"version": "embedding-cluster-v1", "skill_name": "embedding_cluster", "policy_hash": "abc123"},
					},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer worker.Close()

	server := NewServer(config.Config{
		BindAddr:          ":0",
		StoreBackend:      "memory",
		WorkflowEngine:    "noop",
		PythonAIWorkerURL: worker.URL,
	})
	handler := server.Handler()

	catalog := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/skill_policy_catalog", "", http.StatusOK, &catalog)
	if available, _ := catalog["available"].(bool); !available {
		t.Fatalf("expected available skill policy catalog: %+v", catalog)
	}

	validation := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/skill_policies/validate", "", http.StatusOK, &validation)
	if valid, _ := validation["valid"].(bool); !valid {
		t.Fatalf("expected valid skill policy validation: %+v", validation)
	}
}

func TestScenarioEndpoints(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"scenario-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	createBody := `{
	  "scenario_id":"S1",
	  "user_query":"이번 벚꽃 축제 반응 어때?",
	  "query_type":"여론 요약",
	  "interpretation":"전체 여론 및 분위기 파악",
	  "analysis_scope":"축제 기간",
	  "steps":[
	    {
	      "step":1,
	      "function_name":"가비지 필터링",
	      "runtime_skill_name":"garbage_filter",
	      "result_description":"분석 대상 정제"
	    },
	    {
	      "step":2,
	      "function_name":"빈도 기반 키워드 추출",
	      "parameter_text":"top_n=10",
	      "parameters":{"top_n":10},
	      "result_description":"주요 키워드"
	    }
	  ]
	}`

	created := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/scenarios",
		createBody,
		http.StatusCreated,
		&created,
	)
	if created["scenario_id"] != "S1" {
		t.Fatalf("unexpected created scenario: %+v", created)
	}
	if created["planning_mode"] != "strict" {
		t.Fatalf("unexpected planning mode: %+v", created)
	}

	listResponse := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/scenarios",
		"",
		http.StatusOK,
		&listResponse,
	)
	items, ok := listResponse["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("unexpected scenario list response: %+v", listResponse)
	}

	loaded := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/scenarios/S1",
		"",
		http.StatusOK,
		&loaded,
	)
	if loaded["user_query"] != "이번 벚꽃 축제 반응 어때?" {
		t.Fatalf("unexpected loaded scenario: %+v", loaded)
	}
}

func TestListAndProfileValidationEndpoints(t *testing.T) {
	promptsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(promptsDir, "dataset-prepare-anthropic-v1.md"), []byte("---\ntitle: Prepare\noperation: prepare\nstatus: active\nsummary: prepare\n---\n{{raw_text}}"), 0o644); err != nil {
		t.Fatalf("unexpected write prompt error: %v", err)
	}
	if err := os.WriteFile(filepath.Join(promptsDir, "sentiment-anthropic-v1.md"), []byte("---\ntitle: Sentiment\noperation: sentiment\nstatus: active\nsummary: sentiment\n---\n{{text}}"), 0o644); err != nil {
		t.Fatalf("unexpected write prompt error: %v", err)
	}
	profilesPath := filepath.Join(t.TempDir(), "dataset_profiles.json")
	if err := os.WriteFile(profilesPath, []byte(`{
  "defaults":{"unstructured":"default-unstructured-v1"},
  "profiles":{
    "default-unstructured-v1":{
      "profile_id":"default-unstructured-v1",
      "prepare_prompt_version":"dataset-prepare-anthropic-v1",
      "sentiment_prompt_version":"sentiment-anthropic-v1",
      "regex_rule_names":["media_placeholder"],
      "garbage_rule_names":["ad_marker"]
    }
  }
}`), 0o644); err != nil {
		t.Fatalf("unexpected write profile registry error: %v", err)
	}
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"prompt_catalog": []map[string]any{
				{"version": "dataset-prepare-anthropic-v1", "title": "Prepare", "operation": "prepare", "status": "active", "summary": "prepare"},
				{"version": "sentiment-anthropic-v1", "title": "Sentiment", "operation": "sentiment", "status": "active", "summary": "sentiment"},
			},
			"rule_catalog": map[string]any{
				"available_prepare_regex_rule_names": []string{"media_placeholder"},
				"default_prepare_regex_rule_names":   []string{"media_placeholder"},
				"available_garbage_rule_names":       []string{"ad_marker"},
				"default_garbage_rule_names":         []string{"ad_marker"},
			},
		})
	}))
	defer worker.Close()

	server := NewServer(config.Config{
		BindAddr:            ":0",
		StoreBackend:        "memory",
		WorkflowEngine:      "noop",
		DatasetProfilesPath: profilesPath,
		PromptTemplatesDir:  promptsDir,
		PythonAIWorkerURL:   worker.URL,
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"list-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	projectList := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects", "", http.StatusOK, &projectList)
	if items, ok := projectList["items"].([]any); !ok || len(items) != 1 {
		t.Fatalf("expected one project in list: %+v", projectList)
	}

	dataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets",
		`{"name":"issues","data_type":"unstructured"}`,
		http.StatusCreated,
		&dataset,
	)
	datasetID := dataset["dataset_id"].(string)

	datasetList := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/datasets", "", http.StatusOK, &datasetList)
	if items, ok := datasetList["items"].([]any); !ok || len(items) != 1 {
		t.Fatalf("expected one dataset in list: %+v", datasetList)
	}

	version := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		`{"storage_uri":"issues.csv","data_type":"unstructured"}`,
		http.StatusCreated,
		&version,
	)

	versionList := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		"",
		http.StatusOK,
		&versionList,
	)
	if items, ok := versionList["items"].([]any); !ok || len(items) != 1 {
		t.Fatalf("expected one dataset version in list: %+v", versionList)
	}

	validation := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/dataset_profiles/validate", "", http.StatusOK, &validation)
	if validation["valid"] != true {
		t.Fatalf("expected valid dataset profile validation: %+v", validation)
	}
	registry := validation["registry"].(map[string]any)
	if registry["source_path"] != profilesPath {
		t.Fatalf("unexpected profile validation registry source: %+v", registry)
	}
	if promptCatalog, ok := registry["prompt_catalog"].([]any); !ok || len(promptCatalog) != 2 {
		t.Fatalf("expected prompt catalog metadata: %+v", registry)
	}
	if ruleCatalog, ok := registry["rule_catalog"].(map[string]any); !ok || len(ruleCatalog) == 0 {
		t.Fatalf("expected rule catalog metadata: %+v", registry)
	}

	registryOnly := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/dataset_profiles", "", http.StatusOK, &registryOnly)
	if registryOnly["source_path"] != profilesPath {
		t.Fatalf("unexpected dataset profile registry source: %+v", registryOnly)
	}
	if promptCatalog, ok := registryOnly["prompt_catalog"].([]any); !ok || len(promptCatalog) != 2 {
		t.Fatalf("expected prompt catalog in dataset profile registry: %+v", registryOnly)
	}

	promptCatalogResponse := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/prompt_catalog", "", http.StatusOK, &promptCatalogResponse)
	if promptCatalogResponse["source_path"] != promptsDir {
		t.Fatalf("unexpected prompt catalog source path: %+v", promptCatalogResponse)
	}
	if items, ok := promptCatalogResponse["items"].([]any); !ok || len(items) != 2 {
		t.Fatalf("expected prompt catalog items: %+v", promptCatalogResponse)
	}

	ruleCatalogResponse := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/rule_catalog", "", http.StatusOK, &ruleCatalogResponse)
	if ruleCatalogResponse["available"] != true {
		t.Fatalf("expected available rule catalog: %+v", ruleCatalogResponse)
	}
	if ruleCatalog, ok := ruleCatalogResponse["catalog"].(map[string]any); !ok || len(ruleCatalog) == 0 {
		t.Fatalf("expected rule catalog body: %+v", ruleCatalogResponse)
	}
}

func TestScenarioPlanEndpoint(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"scenario-plan-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/scenarios",
		`{
		  "scenario_id":"S1",
		  "user_query":"이번 벚꽃 축제 반응 어때?",
		  "query_type":"여론 요약",
		  "interpretation":"전체 여론 및 분위기 파악",
		  "analysis_scope":"축제 기간",
		  "steps":[
		    {
		      "step":1,
		      "function_name":"가비지 필터링",
		      "result_description":"분석 대상 정제"
		    },
		    {
		      "step":2,
		      "function_name":"빈도 기반 키워드 추출",
		      "parameter_text":"top_n=10",
		      "result_description":"주요 키워드"
		    }
		  ]
		}`,
		http.StatusCreated,
		&map[string]any{},
	)

	dataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets",
		`{"name":"festival","data_type":"unstructured"}`,
		http.StatusCreated,
		&dataset,
	)
	datasetID := dataset["dataset_id"].(string)

	version := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		`{"storage_uri":"festival.csv","data_type":"unstructured"}`,
		http.StatusCreated,
		&version,
	)
	versionID := version["dataset_version_id"].(string)

	response := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/scenarios/S1/plans",
		`{"dataset_version_id":"`+versionID+`"}`,
		http.StatusCreated,
		&response,
	)

	request := response["request"].(map[string]any)
	plan := response["plan"].(map[string]any)
	steps := plan["plan"].(map[string]any)["steps"].([]any)
	if len(steps) != 2 {
		t.Fatalf("unexpected scenario plan steps: %+v", steps)
	}
	if steps[0].(map[string]any)["skill_name"] != "garbage_filter" {
		t.Fatalf("unexpected first skill: %+v", steps[0])
	}
	if steps[1].(map[string]any)["skill_name"] != "keyword_frequency" {
		t.Fatalf("unexpected second skill: %+v", steps[1])
	}
	scenarioContext := request["context"].(map[string]any)["scenario"].(map[string]any)
	if scenarioContext["scenario_id"] != "S1" {
		t.Fatalf("unexpected scenario context: %+v", scenarioContext)
	}
	if scenarioContext["planning_mode"] != "strict" {
		t.Fatalf("unexpected scenario planning mode: %+v", scenarioContext)
	}
}

func TestScenarioImportEndpoint(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"scenario-import-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	imported := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/scenarios/import",
		`{
		  "rows":[
		    {
		      "scenario_id":"S1",
		      "user_query":"이번 벚꽃 축제 반응 어때?",
		      "query_type":"여론 요약",
		      "interpretation":"전체 여론 및 분위기 파악",
		      "analysis_scope":"축제 기간",
		      "step":2,
		      "function_name":"빈도 기반 키워드 추출",
		      "parameter_text":"top_n=10",
		      "result_description":"주요 키워드"
		    },
		    {
		      "scenario_id":"S1",
		      "user_query":"이번 벚꽃 축제 반응 어때?",
		      "query_type":"여론 요약",
		      "interpretation":"전체 여론 및 분위기 파악",
		      "analysis_scope":"축제 기간",
		      "step":1,
		      "function_name":"가비지 필터링",
		      "result_description":"분석 대상 정제"
		    }
		  ]
		}`,
		http.StatusCreated,
		&imported,
	)
	if imported["scenario_count"] != float64(1) || imported["row_count"] != float64(2) {
		t.Fatalf("unexpected import response: %+v", imported)
	}
	items := imported["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("unexpected import items: %+v", imported)
	}
	scenario := items[0].(map[string]any)
	if scenario["planning_mode"] != "strict" {
		t.Fatalf("unexpected imported planning mode: %+v", scenario)
	}
	steps := scenario["steps"].([]any)
	if steps[0].(map[string]any)["step"] != float64(1) {
		t.Fatalf("expected sorted imported steps: %+v", steps)
	}
}

func TestScenarioExecuteEndpoint(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"scenario-execute-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/scenarios",
		`{
		  "scenario_id":"S1",
		  "planning_mode":"strict",
		  "user_query":"이번 벚꽃 축제 반응 어때?",
		  "query_type":"여론 요약",
		  "interpretation":"전체 여론 및 분위기 파악",
		  "analysis_scope":"축제 기간",
		  "steps":[
		    {
		      "step":1,
		      "function_name":"가비지 필터링",
		      "result_description":"분석 대상 정제"
		    },
		    {
		      "step":2,
		      "runtime_skill_name":"issue_evidence_summary",
		      "function_name":"전체 담론 요약",
		      "result_description":"종합 의견 요약"
		    }
		  ]
		}`,
		http.StatusCreated,
		&map[string]any{},
	)

	dataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets",
		`{"name":"festival","data_type":"unstructured"}`,
		http.StatusCreated,
		&dataset,
	)
	datasetID := dataset["dataset_id"].(string)

	version := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		`{"storage_uri":"festival.csv","data_type":"unstructured"}`,
		http.StatusCreated,
		&version,
	)
	versionID := version["dataset_version_id"].(string)

	response := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/scenarios/S1/execute",
		`{"dataset_version_id":"`+versionID+`"}`,
		http.StatusAccepted,
		&response,
	)

	if response["job_id"] == nil {
		t.Fatalf("expected job_id in scenario execute response: %+v", response)
	}
	request := response["request"].(map[string]any)
	plan := response["plan"].(map[string]any)
	execution := response["execution"].(map[string]any)
	if request["goal"] != "이번 벚꽃 축제 반응 어때?" {
		t.Fatalf("unexpected request payload: %+v", request)
	}
	if plan["plan_id"] == "" {
		t.Fatalf("expected plan_id: %+v", plan)
	}
	if execution["status"] != "queued" {
		t.Fatalf("unexpected execution: %+v", execution)
	}
}

func TestResponsesRenderTimestampsInKST(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects",
		`{"name":"timezone-project"}`,
		http.StatusCreated,
		&project,
	)

	createdAt, ok := project["created_at"].(string)
	if !ok {
		t.Fatalf("expected created_at string, got %#v", project["created_at"])
	}
	if !strings.HasSuffix(createdAt, "+09:00") {
		t.Fatalf("expected KST timestamp, got %s", createdAt)
	}
}

func TestUploadDatasetCreatesStoredVersion(t *testing.T) {
	uploadRoot := t.TempDir()
	artifactRoot := t.TempDir()
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
		UploadRoot:     uploadRoot,
		ArtifactRoot:   artifactRoot,
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"upload-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects/"+projectID+"/datasets", `{"name":"upload-dataset","data_type":"unstructured"}`, http.StatusCreated, &dataset)
	datasetID := dataset["dataset_id"].(string)

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	if err := writer.WriteField("data_type", "unstructured"); err != nil {
		t.Fatalf("unexpected write field error: %v", err)
	}
	if err := writer.WriteField("prepare_required", "true"); err != nil {
		t.Fatalf("unexpected write field error: %v", err)
	}
	if err := writer.WriteField("metadata", `{"text_column":"text"}`); err != nil {
		t.Fatalf("unexpected write field error: %v", err)
	}
	fileWriter, err := writer.CreateFormFile("file", "issues.csv")
	if err != nil {
		t.Fatalf("unexpected create form file error: %v", err)
	}
	if _, err := io.WriteString(fileWriter, "text\n결제 오류가 반복 발생했습니다\n"); err != nil {
		t.Fatalf("unexpected file write error: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/projects/"+projectID+"/datasets/"+datasetID+"/uploads", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("unexpected upload status: %d body=%s", recorder.Code, recorder.Body.String())
	}

	var version map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &version); err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}

	storageURI, _ := version["storage_uri"].(string)
	if storageURI == "" {
		t.Fatalf("expected storage_uri in upload response")
	}
	if !strings.HasPrefix(storageURI, uploadRoot) {
		t.Fatalf("unexpected upload path: %s", storageURI)
	}
	if _, err := os.Stat(storageURI); err != nil {
		t.Fatalf("expected uploaded file on disk: %v", err)
	}
	metadata := version["metadata"].(map[string]any)
	upload := metadata["upload"].(map[string]any)
	if upload["stored_filename"] != "issues.csv" {
		t.Fatalf("unexpected stored filename: %#v", upload["stored_filename"])
	}
	if got := filepath.Base(storageURI); got != "issues.csv" {
		t.Fatalf("unexpected storage basename: %s", got)
	}
}

func TestDatasetBuildJobEndpoints(t *testing.T) {
	artifactRoot := t.TempDir()
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"prepare_uri":          filepath.Join(artifactRoot, "prepared.parquet"),
				"prepared_ref":         filepath.Join(artifactRoot, "prepared.parquet"),
				"prepare_format":       "parquet",
				"prepared_text_column": "normalized_text",
			},
		})
	}))
	defer worker.Close()

	server := NewServer(config.Config{
		BindAddr:          ":0",
		StoreBackend:      "memory",
		WorkflowEngine:    "noop",
		PythonAIWorkerURL: worker.URL,
		ArtifactRoot:      artifactRoot,
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"build-job-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects/"+projectID+"/datasets", `{"name":"issues","data_type":"unstructured"}`, http.StatusCreated, &dataset)
	datasetID := dataset["dataset_id"].(string)

	version := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		`{"storage_uri":"issues.csv","data_type":"unstructured","prepare_required":false}`,
		http.StatusCreated,
		&version,
	)
	versionID := version["dataset_version_id"].(string)

	job := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID+"/prepare_jobs",
		`{}`,
		http.StatusAccepted,
		&job,
	)
	jobID := job["job_id"].(string)
	if job["build_type"] != "prepare" {
		t.Fatalf("unexpected build job payload: %+v", job)
	}

	job = waitForBuildJobStatusHTTP(t, handler, projectID, jobID, "completed")
	if job["completed_at"] == nil {
		t.Fatalf("expected completed_at in build job: %+v", job)
	}
	if diagnostics, ok := job["diagnostics"].(map[string]any); !ok || diagnostics["retry_count"] == nil {
		t.Fatalf("expected build job diagnostics: %+v", job)
	}

	listResponse := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID+"/build_jobs",
		"",
		http.StatusOK,
		&listResponse,
	)
	items := listResponse["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("unexpected build job list: %+v", listResponse)
	}
	if diagnostics, ok := items[0].(map[string]any)["diagnostics"].(map[string]any); !ok || diagnostics["retry_count"] == nil {
		t.Fatalf("expected build job diagnostics in list: %+v", listResponse)
	}
}

func TestOpenAPIDocumentAndSwaggerUI(t *testing.T) {
	openapiDir := t.TempDir()
	openapiPath := filepath.Join(openapiDir, "openapi.yaml")
	if err := os.WriteFile(openapiPath, []byte("openapi: 3.1.0\ninfo:\n  title: test\n  version: 0.1.0\n"), 0o644); err != nil {
		t.Fatalf("unexpected openapi write error: %v", err)
	}

	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
		OpenAPIPath:    openapiPath,
	})
	handler := server.Handler()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil)
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected openapi status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "openapi: 3.1.0") {
		t.Fatalf("unexpected openapi body: %s", recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/swagger", nil)
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected swagger status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "SwaggerUIBundle") {
		t.Fatalf("swagger html missing bundle bootstrap")
	}
	if !strings.Contains(body, "/openapi.yaml") {
		t.Fatalf("swagger html missing openapi url")
	}
}

func readJSONResponse(
	t *testing.T,
	handler http.Handler,
	method string,
	path string,
	body string,
	expectedStatus int,
	dest any,
) {
	t.Helper()

	var reader *bytes.Reader
	if body == "" {
		reader = bytes.NewReader(nil)
	} else {
		reader = bytes.NewReader([]byte(body))
	}

	req := httptest.NewRequest(method, path, reader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)

	if recorder.Code != expectedStatus {
		t.Fatalf("unexpected status for %s %s: got=%d body=%s", method, path, recorder.Code, recorder.Body.String())
	}
	if dest == nil {
		return
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), dest); err != nil {
		t.Fatalf("failed to decode response for %s %s: %v", method, path, err)
	}
}

func waitForBuildJobStatusHTTP(t *testing.T, handler http.Handler, projectID, jobID, expectedStatus string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var job map[string]any
		readJSONResponse(
			t,
			handler,
			http.MethodGet,
			"/projects/"+projectID+"/dataset_build_jobs/"+jobID,
			"",
			http.StatusOK,
			&job,
		)
		if job["status"] == expectedStatus {
			return job
		}
		time.Sleep(20 * time.Millisecond)
	}
	var job map[string]any
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/dataset_build_jobs/"+jobID,
		"",
		http.StatusOK,
		&job,
	)
	t.Fatalf("expected build job %s status %s, got %+v", jobID, expectedStatus, job)
	return nil
}
