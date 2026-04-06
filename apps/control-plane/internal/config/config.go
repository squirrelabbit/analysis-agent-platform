package config

import (
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	BindAddr            string
	StoreBackend        string
	DatabaseURL         string
	OpenAPIPath         string
	DatasetProfilesPath string
	DataRoot            string
	UploadRoot          string
	ArtifactRoot        string
	DuckDBPath          string
	PythonAIWorkerURL   string
	PlannerBackend      string
	WorkflowEngine      string
	TemporalAddress     string
	TemporalNamespace   string
	TemporalTaskQueue   string
}

func Load() Config {
	addr := os.Getenv("BIND_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	storeBackend := os.Getenv("STORE_BACKEND")
	if storeBackend == "" {
		storeBackend = "memory"
	}
	workspaceRoot := detectWorkspaceRoot()
	openAPIPath := resolvePath(os.Getenv("OPENAPI_PATH"), filepath.Join(workspaceRoot, "docs", "api", "openapi.yaml"), workspaceRoot)
	datasetProfilesPath := resolvePath(os.Getenv("DATASET_PROFILES_PATH"), filepath.Join(workspaceRoot, "config", "dataset_profiles.json"), workspaceRoot)
	dataRoot := resolvePath(os.Getenv("DATA_ROOT"), filepath.Join(workspaceRoot, "data"), workspaceRoot)
	uploadRoot := resolvePath(os.Getenv("UPLOAD_ROOT"), filepath.Join(dataRoot, "uploads"), workspaceRoot)
	artifactRoot := resolvePath(os.Getenv("ARTIFACT_ROOT"), filepath.Join(dataRoot, "artifacts"), workspaceRoot)
	duckDBPath := os.Getenv("DUCKDB_PATH")
	if duckDBPath == "" {
		duckDBPath = "analysis_support.duckdb"
	}
	duckDBPath = resolvePath(duckDBPath, filepath.Join(workspaceRoot, "analysis_support.duckdb"), workspaceRoot)
	pythonAIWorkerURL := os.Getenv("PYTHON_AI_WORKER_URL")
	if pythonAIWorkerURL == "" {
		pythonAIWorkerURL = "http://127.0.0.1:8090"
	}
	plannerBackend := os.Getenv("PLANNER_BACKEND")
	if plannerBackend == "" {
		plannerBackend = "stub"
	}
	workflowEngine := os.Getenv("WORKFLOW_ENGINE")
	if workflowEngine == "" {
		workflowEngine = "noop"
	}
	temporalAddress := os.Getenv("TEMPORAL_ADDRESS")
	if temporalAddress == "" {
		temporalAddress = "localhost:7233"
	}
	temporalNamespace := os.Getenv("TEMPORAL_NAMESPACE")
	if temporalNamespace == "" {
		temporalNamespace = "default"
	}
	temporalTaskQueue := os.Getenv("TEMPORAL_TASK_QUEUE")
	if temporalTaskQueue == "" {
		temporalTaskQueue = "analysis-support"
	}
	return Config{
		BindAddr:            addr,
		StoreBackend:        storeBackend,
		DatabaseURL:         os.Getenv("DATABASE_URL"),
		OpenAPIPath:         openAPIPath,
		DatasetProfilesPath: datasetProfilesPath,
		DataRoot:            dataRoot,
		UploadRoot:          uploadRoot,
		ArtifactRoot:        artifactRoot,
		DuckDBPath:          duckDBPath,
		PythonAIWorkerURL:   pythonAIWorkerURL,
		PlannerBackend:      plannerBackend,
		WorkflowEngine:      workflowEngine,
		TemporalAddress:     temporalAddress,
		TemporalNamespace:   temporalNamespace,
		TemporalTaskQueue:   temporalTaskQueue,
	}
}

func detectWorkspaceRoot() string {
	cwd, err := os.Getwd()
	if err != nil || strings.TrimSpace(cwd) == "" {
		return "."
	}
	dir := cwd
	for {
		if fileExists(filepath.Join(dir, "compose.dev.yml")) || fileExists(filepath.Join(dir, "AGENTS.md")) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return cwd
		}
		dir = parent
	}
}

func resolvePath(value string, fallback string, workspaceRoot string) string {
	resolved := strings.TrimSpace(value)
	if resolved == "" {
		resolved = fallback
	}
	if filepath.IsAbs(resolved) {
		return resolved
	}
	if strings.TrimSpace(workspaceRoot) == "" {
		return resolved
	}
	return filepath.Join(workspaceRoot, resolved)
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
