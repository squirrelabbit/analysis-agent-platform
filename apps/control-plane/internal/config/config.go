package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Config struct {
	BindAddr                                string
	StoreBackend                            string
	DatabaseURL                             string
	CORSAllowedOrigins                      []string
	OpenAPIPath                             string
	FrontendOpenAPIPath                     string
	DatasetProfilesPath                     string
	PromptTemplatesDir                      string
	DataRoot                                string
	UploadRoot                              string
	ArtifactRoot                            string
	DuckDBPath                              string
	PythonAIWorkerURL                       string
	PlannerBackend                          string
	WorkflowEngine                          string
	TemporalAddress                         string
	TemporalNamespace                       string
	TemporalTaskQueue                       string
	TemporalBuildTaskQueue                  string
	TemporalPersistenceMode                 string
	TemporalRetentionMode                   string
	TemporalRecoveryMode                    string
	TemporalAnalysisMaxConcurrentActivities int
	TemporalBuildMaxConcurrentActivities    int
	DatasetBuildPrepareMaxConcurrent        int
	DatasetBuildSentimentMaxConcurrent      int
	DatasetBuildEmbeddingMaxConcurrent      int
	DatasetBuildClusterMaxConcurrent        int
	AnthropicExecutionTokenCeiling          int
	PythonAIWorkerHTTPTimeoutSec            int
	// silverone 2026-06-08 — plan reuse(POC-1) 기능 토글. follow-up 질의를
	// rule-based로 분류해 이전 plan을 재실행하던 경로가 "TOP N" 류 새 분석 질문을
	// 직전 결과로 오재표시하는 context hijack을 유발해(검증: festival E3→E4 라이브
	// 재현) 기본 OFF로 둔다. ANALYSIS_PLAN_REUSE_ENABLED=true로만 활성.
	PlanReuseEnabled bool
	// silverone 2026-06-08 — LLOA 모델 화면 표시명. artifact view 응답의
	// applied.model_display_name을 빌드 재실행 없이 응답 시점에 입히기 위해
	// control-plane도 이 env를 읽는다. raw model(LLOAModel)이 빌드 당시 summary.model과
	// 일치할 때만 LLOAModelDisplayName을 노출한다(하드코딩 매핑 없음, env 기반).
	LLOAModel            string
	LLOAModelDisplayName string

	// 인증/RBAC (ADR-025, silverone 2026-06-12). AuthEnabled가 false면 auth
	// 미들웨어가 통과(7/30 사내망 격리 단계 호환). Google OIDC 설정 +
	// 허용 도메인 + 초기 admin 이메일 + 세션 TTL.
	AuthEnabled         bool
	AuthGoogleClientID  string
	AuthGoogleSecret    string
	AuthRedirectURL     string   // 예: https://staging/api/auth/google/callback
	AuthPostLoginURL    string   // 로그인 후 프론트 redirect (기본 "/")
	AuthAllowedDomain   string   // 회사 도메인 (hd / email suffix), 비면 도메인 제한 없음
	AuthAdminEmails     []string // 첫 로그인 시 global_role=admin 부여
	AuthSessionTTLHours int
	AuthCookieSecure    bool // 쿠키 Secure 플래그 (https 환경 true)
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
	corsAllowedOrigins := splitCommaSeparated(
		os.Getenv("CORS_ALLOWED_ORIGINS"),
		defaultCORSAllowedOrigins(),
	)
	workspaceRoot := detectWorkspaceRoot()
	openAPIPath := resolvePath(os.Getenv("OPENAPI_PATH"), filepath.Join(workspaceRoot, "docs", "api", "openapi.yaml"), workspaceRoot)
	frontendOpenAPIPath := resolvePath(os.Getenv("FRONTEND_OPENAPI_PATH"), filepath.Join(workspaceRoot, "docs", "api", "openapi.frontend.yaml"), workspaceRoot)
	datasetProfilesPath := resolvePath(os.Getenv("DATASET_PROFILES_PATH"), filepath.Join(workspaceRoot, "config", "dataset_profiles.json"), workspaceRoot)
	promptTemplatesDir := resolvePath(os.Getenv("PYTHON_AI_PROMPTS_DIR"), filepath.Join(workspaceRoot, "config", "prompts"), workspaceRoot)
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
	temporalBuildTaskQueue := os.Getenv("TEMPORAL_BUILD_TASK_QUEUE")
	if temporalBuildTaskQueue == "" {
		temporalBuildTaskQueue = temporalTaskQueue + "-build"
	}
	temporalPersistenceMode := os.Getenv("TEMPORAL_PERSISTENCE_MODE")
	if temporalPersistenceMode == "" {
		temporalPersistenceMode = "dev_ephemeral"
	}
	temporalRetentionMode := os.Getenv("TEMPORAL_RETENTION_MODE")
	if temporalRetentionMode == "" {
		temporalRetentionMode = "temporal_dev_default"
	}
	temporalRecoveryMode := os.Getenv("TEMPORAL_RECOVERY_MODE")
	if temporalRecoveryMode == "" {
		temporalRecoveryMode = "startup_reconciliation"
	}
	analysisMaxConcurrentActivities := envPositiveInt("TEMPORAL_ANALYSIS_MAX_CONCURRENT_ACTIVITIES", 8)
	buildMaxConcurrentActivities := envPositiveInt("TEMPORAL_BUILD_MAX_CONCURRENT_ACTIVITIES", 4)
	prepareMaxConcurrent := envPositiveInt("DATASET_BUILD_PREPARE_MAX_CONCURRENT", 3)
	sentimentMaxConcurrent := envPositiveInt("DATASET_BUILD_SENTIMENT_MAX_CONCURRENT", 2)
	embeddingMaxConcurrent := envPositiveInt("DATASET_BUILD_EMBEDDING_MAX_CONCURRENT", 1)
	clusterMaxConcurrent := envPositiveInt("DATASET_BUILD_CLUSTER_MAX_CONCURRENT", 1)
	anthropicExecutionTokenCeiling := envNonNegativeInt("ANTHROPIC_EXECUTION_TOKEN_CEILING", 0)
	// Python AI worker LLM-backed skills can take well over 30s when the
	// schema strict-mode is in effect (Anthropic JSON schema enforcement
	// adds first-token latency). Default 120s gives Anthropic + worker
	// retry budget without making the dev loop intolerably slow.
	pythonAIWorkerHTTPTimeoutSec := envPositiveInt("PYTHON_AI_WORKER_HTTP_TIMEOUT_SEC", 120)
	return Config{
		BindAddr:                                addr,
		StoreBackend:                            storeBackend,
		DatabaseURL:                             os.Getenv("DATABASE_URL"),
		CORSAllowedOrigins:                      corsAllowedOrigins,
		OpenAPIPath:                             openAPIPath,
		FrontendOpenAPIPath:                     frontendOpenAPIPath,
		DatasetProfilesPath:                     datasetProfilesPath,
		PromptTemplatesDir:                      promptTemplatesDir,
		DataRoot:                                dataRoot,
		UploadRoot:                              uploadRoot,
		ArtifactRoot:                            artifactRoot,
		DuckDBPath:                              duckDBPath,
		PythonAIWorkerURL:                       pythonAIWorkerURL,
		PlannerBackend:                          plannerBackend,
		WorkflowEngine:                          workflowEngine,
		TemporalAddress:                         temporalAddress,
		TemporalNamespace:                       temporalNamespace,
		TemporalTaskQueue:                       temporalTaskQueue,
		TemporalBuildTaskQueue:                  temporalBuildTaskQueue,
		TemporalPersistenceMode:                 temporalPersistenceMode,
		TemporalRetentionMode:                   temporalRetentionMode,
		TemporalRecoveryMode:                    temporalRecoveryMode,
		TemporalAnalysisMaxConcurrentActivities: analysisMaxConcurrentActivities,
		TemporalBuildMaxConcurrentActivities:    buildMaxConcurrentActivities,
		DatasetBuildPrepareMaxConcurrent:        prepareMaxConcurrent,
		DatasetBuildSentimentMaxConcurrent:      sentimentMaxConcurrent,
		DatasetBuildEmbeddingMaxConcurrent:      embeddingMaxConcurrent,
		DatasetBuildClusterMaxConcurrent:        clusterMaxConcurrent,
		AnthropicExecutionTokenCeiling:          anthropicExecutionTokenCeiling,
		PythonAIWorkerHTTPTimeoutSec:            pythonAIWorkerHTTPTimeoutSec,
		PlanReuseEnabled:                        envBool("ANALYSIS_PLAN_REUSE_ENABLED", false),
		LLOAModel:                               strings.TrimSpace(os.Getenv("LLOA_MODEL")),
		LLOAModelDisplayName:                    strings.TrimSpace(os.Getenv("LLOA_MODEL_DISPLAY_NAME")),
		AuthEnabled:                             envBool("AUTH_ENABLED", false),
		AuthGoogleClientID:                      strings.TrimSpace(os.Getenv("AUTH_GOOGLE_CLIENT_ID")),
		AuthGoogleSecret:                        strings.TrimSpace(os.Getenv("AUTH_GOOGLE_CLIENT_SECRET")),
		AuthRedirectURL:                         strings.TrimSpace(os.Getenv("AUTH_REDIRECT_URL")),
		AuthPostLoginURL:                        envString("AUTH_POST_LOGIN_URL", "/"),
		AuthAllowedDomain:                       strings.TrimSpace(os.Getenv("AUTH_ALLOWED_DOMAIN")),
		AuthAdminEmails:                         splitCommaSeparated(os.Getenv("AUTH_ADMIN_EMAILS"), nil),
		AuthSessionTTLHours:                     envPositiveInt("AUTH_SESSION_TTL_HOURS", 168),
		AuthCookieSecure:                        envBool("AUTH_COOKIE_SECURE", false),
	}
}

func envString(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envNonNegativeInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
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

func envPositiveInt(key string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func splitCommaSeparated(value string, fallback []string) []string {
	resolved := strings.TrimSpace(value)
	if resolved == "" {
		return append([]string(nil), fallback...)
	}

	parts := strings.Split(resolved, ",")
	items := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		items = append(items, item)
	}
	if len(items) == 0 {
		return append([]string(nil), fallback...)
	}
	return items
}

func defaultCORSAllowedOrigins() []string {
	return []string{
		"http://127.0.0.1:4173",
		"http://localhost:4173",
		"http://127.0.0.1:5173",
		"http://localhost:5173",
	}
}
