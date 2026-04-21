package http

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
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

	_ "github.com/marcboeker/go-duckdb"
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
	analysis := map[string]any{}
	submitBody := `{
	  "dataset_id":"` + datasetID + `",
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
	if analysis["request"].(map[string]any)["dataset_version_id"] != version["dataset_version_id"] {
		t.Fatalf("expected resolved active dataset version in analysis request: %+v", analysis)
	}
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
	analysis := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/analysis_requests",
		`{"dataset_id":"`+datasetID+`","data_type":"structured","goal":"이슈를 정리해줘"}`,
		http.StatusCreated,
		&analysis,
	)
	if analysis["request"].(map[string]any)["dataset_version_id"] != version["dataset_version_id"] {
		t.Fatalf("expected resolved active dataset version in analysis request: %+v", analysis)
	}
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
	items, ok := projectList["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one project in list: %+v", projectList)
	}
	projectSummary := items[0].(map[string]any)
	if projectSummary["dataset_version_count"] != float64(0) ||
		projectSummary["scenario_count"] != float64(0) ||
		projectSummary["prompt_count"] != float64(0) {
		t.Fatalf("expected empty project counts: %+v", projectSummary)
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
	datasetSummary := datasetList["items"].([]any)[0].(map[string]any)
	if _, ok := datasetSummary["active_dataset_version_id"]; ok {
		t.Fatalf("expected active_dataset_version_id to be omitted before version creation: %+v", datasetSummary)
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
	if version["is_active"] != true {
		t.Fatalf("expected first version to be active: %+v", version)
	}
	firstVersionID := version["dataset_version_id"].(string)

	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		`{"storage_uri":"issues_v2.csv","data_type":"unstructured"}`,
		http.StatusCreated,
		&map[string]any{},
	)

	dataset = map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/datasets/"+datasetID, "", http.StatusOK, &dataset)
	if dataset["active_dataset_version_id"] == nil || dataset["active_dataset_version_id"] == firstVersionID {
		t.Fatalf("expected latest created version to become active: %+v", dataset)
	}
	activeVersionID := dataset["active_dataset_version_id"].(string)

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
	if items, ok := versionList["items"].([]any); !ok || len(items) != 2 {
		t.Fatalf("expected two dataset versions in list: %+v", versionList)
	} else {
		activeCount := 0
		for _, item := range items {
			versionItem := item.(map[string]any)
			if versionItem["is_active"] == true {
				activeCount++
				if versionItem["dataset_version_id"] != activeVersionID {
					t.Fatalf("unexpected active version in list: %+v", versionItem)
				}
			}
		}
		if activeCount != 1 {
			t.Fatalf("expected exactly one active version in list: %+v", versionList)
		}
	}

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
		      "runtime_skill_name":"garbage_filter",
		      "result_description":"분석 대상 정제"
		    }
		  ]
		}`,
		http.StatusCreated,
		&map[string]any{},
	)

	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/prompts",
		`{
		  "version":"project-prepare-v1",
		  "operation":"prepare",
		  "content":"---\ntitle: 프로젝트 전처리\noperation: prepare\nstatus: experimental\nsummary: 프로젝트 전용 전처리\n---\n{{raw_text}}\n"
		}`,
		http.StatusCreated,
		&map[string]any{},
	)

	projectList = map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects", "", http.StatusOK, &projectList)
	items, ok = projectList["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one project in list after resource creation: %+v", projectList)
	}
	projectSummary = items[0].(map[string]any)
	if projectSummary["dataset_version_count"] != float64(2) ||
		projectSummary["scenario_count"] != float64(1) ||
		projectSummary["prompt_count"] != float64(1) {
		t.Fatalf("unexpected aggregated project counts: %+v", projectSummary)
	}

	projectDetail := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID, "", http.StatusOK, &projectDetail)
	if projectDetail["dataset_version_count"] != float64(2) ||
		projectDetail["scenario_count"] != float64(1) ||
		projectDetail["prompt_count"] != float64(1) {
		t.Fatalf("unexpected aggregated project detail counts: %+v", projectDetail)
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

func TestProjectPromptEndpoints(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"prompt-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	created := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/prompts",
		`{
		  "version":"project-prepare-v1",
		  "operation":"prepare",
		  "content":"---\ntitle: 프로젝트 전처리\noperation: prepare\nstatus: experimental\nsummary: 프로젝트 전용 전처리\n---\n{{raw_text}}\n"
		}`,
		http.StatusCreated,
		&created,
	)
	if created["version"] != "project-prepare-v1" || created["operation"] != "prepare" {
		t.Fatalf("unexpected created project prompt: %+v", created)
	}
	if created["title"] != "프로젝트 전처리" || created["status"] != "experimental" {
		t.Fatalf("expected front matter metadata in response: %+v", created)
	}

	duplicateError := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/prompts",
		`{
		  "version":"project-prepare-v1",
		  "operation":"prepare",
		  "content":"---\ntitle: 프로젝트 전처리 2\noperation: prepare\nstatus: experimental\nsummary: 덮어쓰기 시도\n---\n{{raw_text}}\n"
		}`,
		http.StatusConflict,
		&duplicateError,
	)
	if duplicateError["detail"] == "" {
		t.Fatalf("expected duplicate prompt error body: %+v", duplicateError)
	}

	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/prompts",
		`{
		  "version":"project-sentiment-v1",
		  "operation":"sentiment",
		  "content":"---\ntitle: 프로젝트 감성 분석\noperation: sentiment\nstatus: active\nsummary: 프로젝트 전용 감성 분석\n---\n{{text}}\n"
		}`,
		http.StatusCreated,
		&map[string]any{},
	)

	list := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/prompts", "", http.StatusOK, &list)
	items, ok := list["items"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("expected two project prompts: %+v", list)
	}
	item := items[0].(map[string]any)
	if item["content_hash"] == "" {
		t.Fatalf("expected prompt content hash: %+v", item)
	}

	defaults := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/prompt_defaults", "", http.StatusOK, &defaults)
	if defaults["project_id"] != projectID {
		t.Fatalf("unexpected prompt defaults response: %+v", defaults)
	}
	if _, ok := defaults["prepare_prompt_version"]; ok {
		t.Fatalf("expected empty defaults before update: %+v", defaults)
	}

	readJSONResponse(
		t,
		handler,
		http.MethodPut,
		"/projects/"+projectID+"/prompt_defaults",
		`{
		  "prepare_prompt_version":"project-prepare-v1",
		  "sentiment_prompt_version":"project-sentiment-v1"
		}`,
		http.StatusOK,
		&defaults,
	)
	if defaults["prepare_prompt_version"] != "project-prepare-v1" ||
		defaults["sentiment_prompt_version"] != "project-sentiment-v1" {
		t.Fatalf("unexpected updated prompt defaults: %+v", defaults)
	}

	defaults = map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/prompt_defaults", "", http.StatusOK, &defaults)
	if defaults["prepare_prompt_version"] != "project-prepare-v1" ||
		defaults["sentiment_prompt_version"] != "project-sentiment-v1" {
		t.Fatalf("unexpected loaded prompt defaults: %+v", defaults)
	}
}

func TestGlobalPromptEndpoints(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	created := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/prompts",
		`{
		  "version":"dataset-prepare-managed-v1",
		  "operation":"prepare",
		  "content":"---\ntitle: 글로벌 전처리\noperation: prepare\nstatus: active\nsummary: 글로벌 전처리 템플릿\n---\n{{raw_text}}\n"
		}`,
		http.StatusCreated,
		&created,
	)
	promptID := created["prompt_id"].(string)
	if created["version"] != "dataset-prepare-managed-v1" || created["operation"] != "prepare" {
		t.Fatalf("unexpected created global prompt: %+v", created)
	}

	secondCreated := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/prompts",
		`{
		  "version":"dataset-sentiment-managed-v1",
		  "operation":"sentiment",
		  "content":"---\ntitle: 글로벌 감성\noperation: sentiment\nstatus: active\nsummary: 글로벌 감성 템플릿\n---\n{{text}}\n"
		}`,
		http.StatusCreated,
		&secondCreated,
	)

	filtered := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/prompts?operation=prepare", "", http.StatusOK, &filtered)
	items, ok := filtered["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one prepare prompt: %+v", filtered)
	}
	if items[0].(map[string]any)["prompt_id"] != promptID {
		t.Fatalf("unexpected filtered prompt list: %+v", filtered)
	}

	loaded := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/prompts/"+promptID, "", http.StatusOK, &loaded)
	if loaded["title"] != "글로벌 전처리" {
		t.Fatalf("unexpected prompt detail response: %+v", loaded)
	}

	updated := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPatch,
		"/prompts/"+promptID,
		`{
		  "content":"---\ntitle: 글로벌 전처리 수정\noperation: prepare\nstatus: experimental\nsummary: 수정된 글로벌 전처리 템플릿\n---\n{{raw_text}}\n"
		}`,
		http.StatusOK,
		&updated,
	)
	if updated["title"] != "글로벌 전처리 수정" || updated["status"] != "experimental" {
		t.Fatalf("expected updated prompt metadata: %+v", updated)
	}

	readJSONResponse(t, handler, http.MethodDelete, "/prompts/"+promptID, "", http.StatusNoContent, nil)

	deleted := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/prompts/"+promptID, "", http.StatusNotFound, &deleted)
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
	response := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/scenarios/S1/plans",
		`{"dataset_id":"`+datasetID+`"}`,
		http.StatusCreated,
		&response,
	)

	request := response["request"].(map[string]any)
	plan := response["plan"].(map[string]any)
	if request["dataset_version_id"] != version["dataset_version_id"] {
		t.Fatalf("expected scenario plan request to resolve active dataset version: %+v", request)
	}
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
	response := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/scenarios/S1/execute",
		`{"dataset_id":"`+datasetID+`"}`,
		http.StatusAccepted,
		&response,
	)

	if response["job_id"] == nil {
		t.Fatalf("expected job_id in scenario execute response: %+v", response)
	}
	request := response["request"].(map[string]any)
	plan := response["plan"].(map[string]any)
	execution := response["execution"].(map[string]any)
	if request["dataset_version_id"] != version["dataset_version_id"] {
		t.Fatalf("expected scenario execute request to resolve active dataset version: %+v", request)
	}
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
	if err := writer.WriteField("prepare_llm_mode", "disabled"); err != nil {
		t.Fatalf("unexpected write field error: %v", err)
	}
	if err := writer.WriteField("sentiment_llm_mode", "enabled"); err != nil {
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
	if version["prepare_llm_mode"] != "disabled" {
		t.Fatalf("unexpected prepare_llm_mode: %#v", version["prepare_llm_mode"])
	}
	if version["sentiment_llm_mode"] != "enabled" {
		t.Fatalf("unexpected sentiment_llm_mode: %#v", version["sentiment_llm_mode"])
	}
	if version["is_active"] != true {
		t.Fatalf("expected uploaded dataset version to be active: %+v", version)
	}
	if dataset["active_dataset_version_id"] != nil {
		t.Fatalf("expected dataset create response to omit active version before upload: %+v", dataset)
	}
	loadedDataset := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/datasets/"+datasetID, "", http.StatusOK, &loadedDataset)
	if loadedDataset["active_dataset_version_id"] != version["dataset_version_id"] {
		t.Fatalf("expected uploaded version to become active: %+v", loadedDataset)
	}

	req = httptest.NewRequest(http.MethodGet, "/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+version["dataset_version_id"].(string)+"/source_download", nil)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected source download status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if disposition := recorder.Header().Get("Content-Disposition"); !strings.Contains(disposition, "attachment; filename=") || !strings.Contains(disposition, "issues.csv") {
		t.Fatalf("unexpected content-disposition: %s", disposition)
	}
	if got := recorder.Body.String(); got != "text\n결제 오류가 반복 발생했습니다\n" {
		t.Fatalf("unexpected source download body: %q", got)
	}
}

func TestDatasetActiveVersionEndpoints(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:       ":0",
		StoreBackend:   "memory",
		WorkflowEngine: "noop",
	})
	handler := server.Handler()

	project := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"active-version-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects/"+projectID+"/datasets", `{"name":"active-version-dataset","data_type":"unstructured"}`, http.StatusCreated, &dataset)
	datasetID := dataset["dataset_id"].(string)

	firstVersion := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		`{"storage_uri":"issues-v1.csv","data_type":"unstructured"}`,
		http.StatusCreated,
		&firstVersion,
	)

	secondVersion := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		`{"storage_uri":"issues-v2.csv","data_type":"unstructured","activate_on_create":false}`,
		http.StatusCreated,
		&secondVersion,
	)
	if secondVersion["is_active"] != false {
		t.Fatalf("expected second version to stay inactive: %+v", secondVersion)
	}

	updatedDataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPut,
		"/projects/"+projectID+"/datasets/"+datasetID+"/active_version",
		fmt.Sprintf(`{"dataset_version_id":"%s"}`, secondVersion["dataset_version_id"]),
		http.StatusOK,
		&updatedDataset,
	)
	if updatedDataset["active_dataset_version_id"] != secondVersion["dataset_version_id"] {
		t.Fatalf("expected second version to become active: %+v", updatedDataset)
	}

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
	items := versionList["items"].([]any)
	for _, item := range items {
		versionItem := item.(map[string]any)
		switch versionItem["dataset_version_id"] {
		case firstVersion["dataset_version_id"]:
			if versionItem["is_active"] != false {
				t.Fatalf("expected first version to be inactive: %+v", versionItem)
			}
		case secondVersion["dataset_version_id"]:
			if versionItem["is_active"] != true {
				t.Fatalf("expected second version to be active: %+v", versionItem)
			}
		}
	}

	deactivatedDataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodDelete,
		"/projects/"+projectID+"/datasets/"+datasetID+"/active_version",
		"",
		http.StatusOK,
		&deactivatedDataset,
	)
	if _, ok := deactivatedDataset["active_dataset_version_id"]; ok {
		t.Fatalf("expected active_dataset_version_id to be omitted after deactivation: %+v", deactivatedDataset)
	}
}

func TestDeleteDatasetVersionEndpoint(t *testing.T) {
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
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"delete-version-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(t, handler, http.MethodPost, "/projects/"+projectID+"/datasets", `{"name":"delete-version-dataset","data_type":"unstructured"}`, http.StatusCreated, &dataset)
	datasetID := dataset["dataset_id"].(string)

	version := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions",
		`{"storage_uri":"delete-version-source.jsonl","data_type":"unstructured"}`,
		http.StatusCreated,
		&version,
	)
	versionID := version["dataset_version_id"].(string)

	uploadVersionPath := filepath.Join(uploadRoot, "projects", projectID, "datasets", datasetID, "versions", versionID)
	artifactVersionPath := filepath.Join(artifactRoot, "projects", projectID, "datasets", datasetID, "versions", versionID)
	if err := os.MkdirAll(filepath.Join(uploadVersionPath, "source"), 0o755); err != nil {
		t.Fatalf("failed to seed version upload path: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(artifactVersionPath, "prepare"), 0o755); err != nil {
		t.Fatalf("failed to seed version artifact path: %v", err)
	}

	readJSONResponse(
		t,
		handler,
		http.MethodDelete,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID,
		"",
		http.StatusNoContent,
		nil,
	)

	deletedVersion := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID,
		"",
		http.StatusNotFound,
		&deletedVersion,
	)
	loadedDataset := map[string]any{}
	readJSONResponse(t, handler, http.MethodGet, "/projects/"+projectID+"/datasets/"+datasetID, "", http.StatusOK, &loadedDataset)
	if _, ok := loadedDataset["active_dataset_version_id"]; ok {
		t.Fatalf("expected active version to be cleared after deleting active version: %+v", loadedDataset)
	}
	if _, err := os.Stat(uploadVersionPath); !os.IsNotExist(err) {
		t.Fatalf("expected upload version path to be removed: %v", err)
	}
	if _, err := os.Stat(artifactVersionPath); !os.IsNotExist(err) {
		t.Fatalf("expected artifact version path to be removed: %v", err)
	}
}

func TestDeleteProjectAndDatasetEndpoints(t *testing.T) {
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
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"delete-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

	dataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets",
		`{"name":"delete-dataset","data_type":"unstructured"}`,
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
		`{"storage_uri":"delete-source.jsonl","data_type":"unstructured"}`,
		http.StatusCreated,
		&version,
	)

	datasetPath := filepath.Join(uploadRoot, "projects", projectID, "datasets", datasetID)
	artifactPath := filepath.Join(artifactRoot, "projects", projectID, "datasets", datasetID)
	if err := os.MkdirAll(filepath.Join(datasetPath, "stub"), 0o755); err != nil {
		t.Fatalf("failed to seed dataset upload path: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(artifactPath, "stub"), 0o755); err != nil {
		t.Fatalf("failed to seed dataset artifact path: %v", err)
	}

	readJSONResponse(
		t,
		handler,
		http.MethodDelete,
		"/projects/"+projectID+"/datasets/"+datasetID,
		"",
		http.StatusNoContent,
		nil,
	)

	deletedDataset := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/datasets/"+datasetID,
		"",
		http.StatusNotFound,
		&deletedDataset,
	)
	if _, err := os.Stat(datasetPath); !os.IsNotExist(err) {
		t.Fatalf("expected upload dataset path to be removed: %v", err)
	}
	if _, err := os.Stat(artifactPath); !os.IsNotExist(err) {
		t.Fatalf("expected artifact dataset path to be removed: %v", err)
	}

	readJSONResponse(
		t,
		handler,
		http.MethodDelete,
		"/projects/"+projectID,
		"",
		http.StatusNoContent,
		nil,
	)

	deletedProject := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID,
		"",
		http.StatusNotFound,
		&deletedProject,
	)
	projectPath := filepath.Join(uploadRoot, "projects", projectID)
	projectArtifactPath := filepath.Join(artifactRoot, "projects", projectID)
	if _, err := os.Stat(projectPath); !os.IsNotExist(err) {
		t.Fatalf("expected project upload path to be removed: %v", err)
	}
	if _, err := os.Stat(projectArtifactPath); !os.IsNotExist(err) {
		t.Fatalf("expected project artifact path to be removed: %v", err)
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

func TestPreparePreviewAndDownloadEndpoints(t *testing.T) {
	artifactRoot := t.TempDir()
	preparedPath := ""
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"artifact": map[string]any{
				"prepare_uri":          preparedPath,
				"prepared_ref":         preparedPath,
				"prepare_format":       "parquet",
				"prepared_text_column": "normalized_text",
				"row_id_column":        "row_id",
				"summary": map[string]any{
					"input_row_count":  3,
					"output_row_count": 2,
					"kept_count":       1,
					"review_count":     1,
					"dropped_count":    1,
					"prepare_regex_rule_hits": map[string]any{
						"html_artifact": 1,
						"url_cleanup":   1,
					},
				},
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
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"prepare-preview-project"}`, http.StatusCreated, &project)
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

	preparedPath = filepath.Join(artifactRoot, "projects", projectID, "datasets", datasetID, "versions", versionID, "prepare", "prepared.parquet")
	if err := os.MkdirAll(filepath.Dir(preparedPath), 0o755); err != nil {
		t.Fatalf("unexpected mkdir error: %v", err)
	}
	writePreparedPreviewParquetForHTTP(t, preparedPath, []map[string]any{
		{
			"source_row_index":    0,
			"row_id":              "row-0",
			"raw_text":            "결제 오류가 반복 발생했습니다!!!",
			"normalized_text":     "결제 오류가 반복 발생했습니다.",
			"prepare_disposition": "keep",
			"prepare_reason":      "noise removed",
		},
		{
			"source_row_index":    2,
			"row_id":              "row-2",
			"raw_text":            "로그인이 자주 실패하고 오류가 보입니다",
			"normalized_text":     "로그인이 자주 실패하고 오류가 보입니다.",
			"prepare_disposition": "review",
			"prepare_reason":      "needs review",
		},
	})

	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID+"/prepare",
		`{}`,
		http.StatusAccepted,
		nil,
	)

	loadedVersion := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID,
		"",
		http.StatusOK,
		&loadedVersion,
	)
	prepareSummary, ok := loadedVersion["prepare_summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected prepare_summary in dataset version: %+v", loadedVersion)
	}
	if prepareSummary["review_count"] != float64(1) {
		t.Fatalf("unexpected prepare_summary payload: %+v", prepareSummary)
	}

	preview := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID+"/prepare_preview?limit=2",
		"",
		http.StatusOK,
		&preview,
	)
	if preview["prepared_ref"] != preparedPath {
		t.Fatalf("unexpected prepared_ref: %+v", preview)
	}
	if preview["sample_limit"] != float64(2) {
		t.Fatalf("unexpected sample_limit: %+v", preview)
	}
	samples, ok := preview["samples"].([]any)
	if !ok || len(samples) != 2 {
		t.Fatalf("unexpected samples payload: %+v", preview)
	}
	warningPanel, ok := preview["warning_panel"].(map[string]any)
	if !ok || warningPanel["review_count"] != float64(1) {
		t.Fatalf("unexpected warning_panel payload: %+v", preview)
	}

	req := httptest.NewRequest(http.MethodGet, "/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID+"/prepare_download", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected download status: got=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "text/csv; charset=utf-8" {
		t.Fatalf("unexpected content-type: %s", contentType)
	}
	if disposition := recorder.Header().Get("Content-Disposition"); !strings.Contains(disposition, "attachment; filename=") || !strings.Contains(disposition, "prepared.csv") {
		t.Fatalf("unexpected content-disposition: %s", disposition)
	}
	body := recorder.Body.Bytes()
	if len(body) < 3 || !bytes.Equal(body[:3], []byte{0xEF, 0xBB, 0xBF}) {
		t.Fatalf("expected utf-8 bom in csv export: %v", body)
	}
	csvBody := string(body[3:])
	if !strings.Contains(csvBody, "source_row_index,row_id,raw_text,normalized_text,prepare_disposition,prepare_reason") {
		t.Fatalf("unexpected csv header: %s", csvBody)
	}
	if !strings.Contains(csvBody, "row-0") || !strings.Contains(csvBody, "row-2") {
		t.Fatalf("unexpected csv rows: %s", csvBody)
	}
}

func TestSentimentPreviewAndDownloadEndpoints(t *testing.T) {
	artifactRoot := t.TempDir()
	sentimentPath := ""
	worker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tasks/sentiment_label":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"artifact": map[string]any{
					"sentiment_uri":               sentimentPath,
					"sentiment_ref":               sentimentPath,
					"sentiment_format":            "parquet",
					"sentiment_label_column":      "sentiment_label",
					"sentiment_confidence_column": "sentiment_confidence",
					"sentiment_reason_column":     "sentiment_reason",
					"row_id_column":               "row_id",
					"summary": map[string]any{
						"input_row_count":      3,
						"labeled_row_count":    2,
						"text_column":          "text",
						"sentiment_batch_size": 8,
						"label_counts": map[string]any{
							"negative": 1,
							"neutral":  1,
						},
					},
				},
			})
		default:
			t.Fatalf("unexpected worker path: %s", r.URL.Path)
		}
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
	readJSONResponse(t, handler, http.MethodPost, "/projects", `{"name":"sentiment-preview-project"}`, http.StatusCreated, &project)
	projectID := project["project_id"].(string)

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

	sentimentPath = filepath.Join(artifactRoot, "projects", projectID, "datasets", datasetID, "versions", versionID, "sentiment", "sentiment.parquet")
	if err := os.MkdirAll(filepath.Dir(sentimentPath), 0o755); err != nil {
		t.Fatalf("unexpected mkdir error: %v", err)
	}
	writeSentimentPreviewParquetForHTTP(t, sentimentPath, []map[string]any{
		{
			"source_row_index":         0,
			"row_id":                   "row-0",
			"sentiment_label":          "negative",
			"sentiment_confidence":     0.91,
			"sentiment_reason":         "결제 실패와 오류 반복 언급",
			"sentiment_prompt_version": "sentiment-anthropic-v2",
		},
		{
			"source_row_index":         2,
			"row_id":                   "row-2",
			"sentiment_label":          "neutral",
			"sentiment_confidence":     0.64,
			"sentiment_reason":         "상태 설명 중심",
			"sentiment_prompt_version": "sentiment-anthropic-v2",
		},
	})

	readJSONResponse(
		t,
		handler,
		http.MethodPost,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID+"/sentiment",
		`{}`,
		http.StatusAccepted,
		nil,
	)

	preview := map[string]any{}
	readJSONResponse(
		t,
		handler,
		http.MethodGet,
		"/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID+"/sentiment_preview?limit=2",
		"",
		http.StatusOK,
		&preview,
	)
	if preview["sentiment_ref"] != sentimentPath {
		t.Fatalf("unexpected sentiment_ref: %+v", preview)
	}
	if preview["sample_limit"] != float64(2) {
		t.Fatalf("unexpected sample_limit: %+v", preview)
	}
	summary, ok := preview["summary"].(map[string]any)
	if !ok || summary["labeled_row_count"] != float64(2) {
		t.Fatalf("unexpected summary payload: %+v", preview)
	}
	samples, ok := preview["samples"].([]any)
	if !ok || len(samples) != 2 {
		t.Fatalf("unexpected samples payload: %+v", preview)
	}

	req := httptest.NewRequest(http.MethodGet, "/projects/"+projectID+"/datasets/"+datasetID+"/versions/"+versionID+"/sentiment_download", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected download status: got=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "text/csv; charset=utf-8" {
		t.Fatalf("unexpected content-type: %s", contentType)
	}
	if disposition := recorder.Header().Get("Content-Disposition"); !strings.Contains(disposition, "attachment; filename=") || !strings.Contains(disposition, "sentiment.csv") {
		t.Fatalf("unexpected content-disposition: %s", disposition)
	}
	body := recorder.Body.Bytes()
	if len(body) < 3 || !bytes.Equal(body[:3], []byte{0xEF, 0xBB, 0xBF}) {
		t.Fatalf("expected utf-8 bom in csv export: %v", body)
	}
	csvBody := string(body[3:])
	if !strings.Contains(csvBody, "source_row_index,row_id,sentiment_label,sentiment_confidence,sentiment_reason,sentiment_prompt_version") {
		t.Fatalf("unexpected csv header: %s", csvBody)
	}
	if !strings.Contains(csvBody, "row-0") || !strings.Contains(csvBody, "row-2") {
		t.Fatalf("unexpected csv rows: %s", csvBody)
	}
}

func TestOpenAPIDocumentAndSwaggerUI(t *testing.T) {
	openapiDir := t.TempDir()
	openapiPath := filepath.Join(openapiDir, "openapi.yaml")
	frontendOpenAPIPath := filepath.Join(openapiDir, "openapi.frontend.yaml")
	if err := os.WriteFile(openapiPath, []byte("openapi: 3.1.0\ninfo:\n  title: test\n  version: 0.1.0\n"), 0o644); err != nil {
		t.Fatalf("unexpected openapi write error: %v", err)
	}
	if err := os.WriteFile(frontendOpenAPIPath, []byte("openapi: 3.1.0\ninfo:\n  title: frontend-test\n  version: 0.1.0\n"), 0o644); err != nil {
		t.Fatalf("unexpected frontend openapi write error: %v", err)
	}

	server := NewServer(config.Config{
		BindAddr:            ":0",
		StoreBackend:        "memory",
		WorkflowEngine:      "noop",
		OpenAPIPath:         openapiPath,
		FrontendOpenAPIPath: frontendOpenAPIPath,
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
	req = httptest.NewRequest(http.MethodGet, "/openapi.frontend.yaml", nil)
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected frontend openapi status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "frontend-test") {
		t.Fatalf("unexpected frontend openapi body: %s", recorder.Body.String())
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

	recorder = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/swagger/frontend", nil)
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected frontend swagger status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	body = recorder.Body.String()
	if !strings.Contains(body, "SwaggerUIBundle") {
		t.Fatalf("frontend swagger html missing bundle bootstrap")
	}
	if !strings.Contains(body, "/openapi.frontend.yaml") {
		t.Fatalf("frontend swagger html missing frontend openapi url")
	}
}

func TestCORSPreflightAllowsConfiguredOrigin(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:           ":0",
		StoreBackend:       "memory",
		WorkflowEngine:     "noop",
		CORSAllowedOrigins: []string{"http://127.0.0.1:4173"},
	})
	handler := server.Handler()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodOptions, "/projects", nil)
	req.Header.Set("Origin", "http://127.0.0.1:4173")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "content-type,authorization")
	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("unexpected preflight status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Access-Control-Allow-Origin"); got != "http://127.0.0.1:4173" {
		t.Fatalf("unexpected allow-origin header: %q", got)
	}
	if got := recorder.Header().Get("Access-Control-Allow-Methods"); !strings.Contains(got, http.MethodPost) {
		t.Fatalf("unexpected allow-methods header: %q", got)
	}
	if got := recorder.Header().Get("Access-Control-Allow-Headers"); got != "content-type,authorization" {
		t.Fatalf("unexpected allow-headers header: %q", got)
	}
}

func TestCORSDoesNotExposeHeadersForUnknownOrigin(t *testing.T) {
	server := NewServer(config.Config{
		BindAddr:           ":0",
		StoreBackend:       "memory",
		WorkflowEngine:     "noop",
		CORSAllowedOrigins: []string{"http://127.0.0.1:4173"},
	})
	handler := server.Handler()

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Origin", "http://localhost:3000")
	handler.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected health status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("unexpected allow-origin header for unknown origin: %q", got)
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

func writePreparedPreviewParquetForHTTP(t *testing.T, path string, rows []map[string]any) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "prepare-preview-http.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("unexpected duckdb open error: %v", err)
	}
	defer db.Close()

	selects := make([]string, 0, len(rows))
	for _, row := range rows {
		selects = append(selects, fmt.Sprintf(
			`SELECT %d AS source_row_index, '%s' AS row_id, '%s' AS raw_text, '%s' AS normalized_text, '%s' AS prepare_disposition, '%s' AS prepare_reason`,
			row["source_row_index"].(int),
			escapeDuckDBLiteralForHTTP(row["row_id"].(string)),
			escapeDuckDBLiteralForHTTP(row["raw_text"].(string)),
			escapeDuckDBLiteralForHTTP(row["normalized_text"].(string)),
			escapeDuckDBLiteralForHTTP(row["prepare_disposition"].(string)),
			escapeDuckDBLiteralForHTTP(row["prepare_reason"].(string)),
		))
	}
	query := fmt.Sprintf(
		`COPY (%s) TO '%s' (FORMAT PARQUET)`,
		strings.Join(selects, " UNION ALL "),
		escapeDuckDBLiteralForHTTP(path),
	)
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("unexpected parquet write error: %v", err)
	}
}

func writeSentimentPreviewParquetForHTTP(t *testing.T, path string, rows []map[string]any) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "sentiment-preview-http.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("unexpected duckdb open error: %v", err)
	}
	defer db.Close()

	selects := make([]string, 0, len(rows))
	for _, row := range rows {
		selects = append(selects, fmt.Sprintf(
			`SELECT %d AS source_row_index, '%s' AS row_id, '%s' AS sentiment_label, %f AS sentiment_confidence, '%s' AS sentiment_reason, '%s' AS sentiment_prompt_version`,
			row["source_row_index"].(int),
			escapeDuckDBLiteralForHTTP(row["row_id"].(string)),
			escapeDuckDBLiteralForHTTP(row["sentiment_label"].(string)),
			row["sentiment_confidence"].(float64),
			escapeDuckDBLiteralForHTTP(row["sentiment_reason"].(string)),
			escapeDuckDBLiteralForHTTP(row["sentiment_prompt_version"].(string)),
		))
	}
	query := fmt.Sprintf(
		`COPY (%s) TO '%s' (FORMAT PARQUET)`,
		strings.Join(selects, " UNION ALL "),
		escapeDuckDBLiteralForHTTP(path),
	)
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("unexpected parquet write error: %v", err)
	}
}

func escapeDuckDBLiteralForHTTP(value string) string {
	return strings.ReplaceAll(value, `'`, `''`)
}
