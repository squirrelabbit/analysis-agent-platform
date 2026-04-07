package domain

import "time"

type Project struct {
	ProjectID   string    `json:"project_id"`
	Name        string    `json:"name"`
	Description *string   `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

type ProjectCreateRequest struct {
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty"`
}

type ProjectListResponse struct {
	Items []Project `json:"items"`
}

type Scenario struct {
	ScenarioID     string         `json:"scenario_id"`
	ProjectID      string         `json:"project_id"`
	PlanningMode   string         `json:"planning_mode"`
	UserQuery      string         `json:"user_query"`
	QueryType      string         `json:"query_type"`
	Interpretation string         `json:"interpretation"`
	AnalysisScope  string         `json:"analysis_scope"`
	Steps          []ScenarioStep `json:"steps"`
	CreatedAt      time.Time      `json:"created_at"`
}

type ScenarioStep struct {
	Step              int            `json:"step"`
	FunctionName      string         `json:"function_name"`
	RuntimeSkillName  *string        `json:"runtime_skill_name,omitempty"`
	ParameterText     *string        `json:"parameter_text,omitempty"`
	Parameters        map[string]any `json:"parameters,omitempty"`
	ResultDescription string         `json:"result_description"`
}

type ScenarioCreateRequest struct {
	ScenarioID     string         `json:"scenario_id"`
	PlanningMode   *string        `json:"planning_mode,omitempty"`
	UserQuery      string         `json:"user_query"`
	QueryType      string         `json:"query_type"`
	Interpretation string         `json:"interpretation"`
	AnalysisScope  string         `json:"analysis_scope"`
	Steps          []ScenarioStep `json:"steps"`
}

type ScenarioListResponse struct {
	Items []Scenario `json:"items"`
}

type ScenarioImportRow struct {
	ScenarioID        string         `json:"scenario_id"`
	PlanningMode      *string        `json:"planning_mode,omitempty"`
	UserQuery         string         `json:"user_query"`
	QueryType         string         `json:"query_type"`
	Interpretation    string         `json:"interpretation"`
	AnalysisScope     string         `json:"analysis_scope"`
	Step              int            `json:"step"`
	FunctionName      string         `json:"function_name"`
	RuntimeSkillName  *string        `json:"runtime_skill_name,omitempty"`
	ParameterText     *string        `json:"parameter_text,omitempty"`
	Parameters        map[string]any `json:"parameters,omitempty"`
	ResultDescription string         `json:"result_description"`
}

type ScenarioImportRequest struct {
	Rows []ScenarioImportRow `json:"rows"`
}

type ScenarioImportResponse struct {
	ScenarioCount int        `json:"scenario_count"`
	RowCount      int        `json:"row_count"`
	Items         []Scenario `json:"items"`
}

type ScenarioPlanCreateRequest struct {
	DatasetVersionID string         `json:"dataset_version_id"`
	Goal             *string        `json:"goal,omitempty"`
	Constraints      []string       `json:"constraints,omitempty"`
	Context          map[string]any `json:"context,omitempty"`
}

type Dataset struct {
	DatasetID   string    `json:"dataset_id"`
	ProjectID   string    `json:"project_id"`
	Name        string    `json:"name"`
	Description *string   `json:"description,omitempty"`
	DataType    string    `json:"data_type"`
	CreatedAt   time.Time `json:"created_at"`
}

type DatasetCreateRequest struct {
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty"`
	DataType    *string `json:"data_type,omitempty"`
}

type DatasetListResponse struct {
	Items []Dataset `json:"items"`
}

type DatasetProfile struct {
	ProfileID              string   `json:"profile_id"`
	PreparePromptVersion   *string  `json:"prepare_prompt_version,omitempty"`
	SentimentPromptVersion *string  `json:"sentiment_prompt_version,omitempty"`
	RegexRuleNames         []string `json:"regex_rule_names,omitempty"`
	GarbageRuleNames       []string `json:"garbage_rule_names,omitempty"`
	EmbeddingModel         *string  `json:"embedding_model,omitempty"`
}

type DatasetVersion struct {
	DatasetVersionID   string          `json:"dataset_version_id"`
	DatasetID          string          `json:"dataset_id"`
	ProjectID          string          `json:"project_id"`
	StorageURI         string          `json:"storage_uri"`
	DataType           string          `json:"data_type"`
	RecordCount        *int            `json:"record_count,omitempty"`
	Metadata           map[string]any  `json:"metadata"`
	Profile            *DatasetProfile `json:"profile,omitempty"`
	PrepareStatus      string          `json:"prepare_status"`
	PrepareModel       *string         `json:"prepare_model,omitempty"`
	PreparePromptVer   *string         `json:"prepare_prompt_version,omitempty"`
	PrepareURI         *string         `json:"prepare_uri,omitempty"`
	PreparedAt         *time.Time      `json:"prepared_at,omitempty"`
	SentimentStatus    string          `json:"sentiment_status"`
	SentimentModel     *string         `json:"sentiment_model,omitempty"`
	SentimentURI       *string         `json:"sentiment_uri,omitempty"`
	SentimentLabeledAt *time.Time      `json:"sentiment_labeled_at,omitempty"`
	SentimentPromptVer *string         `json:"sentiment_prompt_version,omitempty"`
	EmbeddingStatus    string          `json:"embedding_status"`
	EmbeddingModel     *string         `json:"embedding_model,omitempty"`
	EmbeddingURI       *string         `json:"embedding_uri,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
	ReadyAt            *time.Time      `json:"ready_at,omitempty"`
}

type DatasetVersionCreateRequest struct {
	StorageURI        string          `json:"storage_uri"`
	DataType          *string         `json:"data_type,omitempty"`
	RecordCount       *int            `json:"record_count,omitempty"`
	Metadata          map[string]any  `json:"metadata,omitempty"`
	Profile           *DatasetProfile `json:"profile,omitempty"`
	PrepareRequired   *bool           `json:"prepare_required,omitempty"`
	PrepareModel      *string         `json:"prepare_model,omitempty"`
	SentimentRequired *bool           `json:"sentiment_required,omitempty"`
	SentimentModel    *string         `json:"sentiment_model,omitempty"`
	EmbeddingRequired *bool           `json:"embedding_required,omitempty"`
	EmbeddingModel    *string         `json:"embedding_model,omitempty"`
}

type DatasetVersionListResponse struct {
	Items []DatasetVersion `json:"items"`
}

type DatasetPrepareRequest struct {
	TextColumn *string `json:"text_column,omitempty"`
	OutputPath *string `json:"output_path,omitempty"`
	Model      *string `json:"model,omitempty"`
	Force      *bool   `json:"force,omitempty"`
}

type DatasetEmbeddingBuildRequest struct {
	TextColumn       *string `json:"text_column,omitempty"`
	EmbeddingModel   *string `json:"embedding_model,omitempty"`
	DebugExportJSONL *bool   `json:"debug_export_jsonl,omitempty"`
	Force            *bool   `json:"force,omitempty"`
}

type DatasetSentimentBuildRequest struct {
	TextColumn *string `json:"text_column,omitempty"`
	OutputPath *string `json:"output_path,omitempty"`
	Model      *string `json:"model,omitempty"`
	Force      *bool   `json:"force,omitempty"`
}

type DatasetBuildJob struct {
	JobID                 string               `json:"job_id"`
	ProjectID             string               `json:"project_id"`
	DatasetID             string               `json:"dataset_id"`
	DatasetVersionID      string               `json:"dataset_version_id"`
	BuildType             string               `json:"build_type"`
	Status                string               `json:"status"`
	Request               map[string]any       `json:"request,omitempty"`
	TriggeredBy           string               `json:"triggered_by,omitempty"`
	WorkflowID            *string              `json:"workflow_id,omitempty"`
	WorkflowRunID         *string              `json:"workflow_run_id,omitempty"`
	Attempt               int                  `json:"attempt"`
	LastErrorType         *string              `json:"last_error_type,omitempty"`
	ResumedExecutionCount int                  `json:"resumed_execution_count"`
	CreatedAt             time.Time            `json:"created_at"`
	StartedAt             *time.Time           `json:"started_at,omitempty"`
	CompletedAt           *time.Time           `json:"completed_at,omitempty"`
	ErrorMessage          *string              `json:"error_message,omitempty"`
	Diagnostics           *BuildJobDiagnostics `json:"diagnostics,omitempty"`
}

type DatasetBuildJobListResponse struct {
	Items []DatasetBuildJob `json:"items"`
}

type PromptTemplateMetadata struct {
	Version       string   `json:"version"`
	Title         string   `json:"title,omitempty"`
	Operation     string   `json:"operation,omitempty"`
	Status        string   `json:"status,omitempty"`
	Summary       string   `json:"summary,omitempty"`
	DefaultGroups []string `json:"default_groups,omitempty"`
}

type DatasetProfileRuleCatalog struct {
	AvailablePrepareRegexRuleNames []string `json:"available_prepare_regex_rule_names,omitempty"`
	DefaultPrepareRegexRuleNames   []string `json:"default_prepare_regex_rule_names,omitempty"`
	AvailableGarbageRuleNames      []string `json:"available_garbage_rule_names,omitempty"`
	DefaultGarbageRuleNames        []string `json:"default_garbage_rule_names,omitempty"`
}

type DatasetProfileRegistryView struct {
	SourcePath              string                     `json:"source_path,omitempty"`
	PromptTemplatesDir      string                     `json:"prompt_templates_dir,omitempty"`
	Defaults                map[string]string          `json:"defaults,omitempty"`
	Profiles                map[string]DatasetProfile  `json:"profiles,omitempty"`
	AvailablePromptVersions []string                   `json:"available_prompt_versions,omitempty"`
	PromptCatalog           []PromptTemplateMetadata   `json:"prompt_catalog,omitempty"`
	RuleCatalog             *DatasetProfileRuleCatalog `json:"rule_catalog,omitempty"`
}

type DatasetProfileValidationIssue struct {
	Severity    string `json:"severity"`
	Code        string `json:"code"`
	Message     string `json:"message"`
	Scope       string `json:"scope,omitempty"`
	ResourceRef string `json:"resource_ref,omitempty"`
}

type DatasetProfileValidationResponse struct {
	Registry DatasetProfileRegistryView      `json:"registry"`
	Valid    bool                            `json:"valid"`
	Issues   []DatasetProfileValidationIssue `json:"issues,omitempty"`
}

type BuildJobDiagnostics struct {
	RetryCount            int     `json:"retry_count"`
	LastErrorType         *string `json:"last_error_type,omitempty"`
	LastErrorMessage      *string `json:"last_error_message,omitempty"`
	WorkflowID            *string `json:"workflow_id,omitempty"`
	WorkflowRunID         *string `json:"workflow_run_id,omitempty"`
	ResumedExecutionCount int     `json:"resumed_execution_count"`
}

type EmbeddingIndexChunk struct {
	ChunkID          string         `json:"chunk_id"`
	DatasetVersionID string         `json:"dataset_version_id"`
	RowID            string         `json:"row_id,omitempty"`
	SourceRowIndex   int64          `json:"source_row_index,omitempty"`
	ChunkIndex       int            `json:"chunk_index,omitempty"`
	ChunkRef         string         `json:"chunk_ref,omitempty"`
	EmbeddingModel   string         `json:"embedding_model,omitempty"`
	VectorDim        int            `json:"vector_dim"`
	Embedding        []float32      `json:"embedding"`
	Metadata         map[string]any `json:"metadata,omitempty"`
}

type SkillPlanStep struct {
	StepID      string         `json:"step_id"`
	SkillName   string         `json:"skill_name"`
	DatasetName string         `json:"dataset_name"`
	Inputs      map[string]any `json:"inputs"`
}

type SkillPlan struct {
	PlanID    string          `json:"plan_id"`
	Steps     []SkillPlanStep `json:"steps"`
	Notes     *string         `json:"notes,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

type AnalysisRequest struct {
	RequestID        string         `json:"request_id"`
	ProjectID        string         `json:"project_id"`
	DatasetName      *string        `json:"dataset_name,omitempty"`
	DatasetVersionID *string        `json:"dataset_version_id,omitempty"`
	Goal             string         `json:"goal"`
	Constraints      []string       `json:"constraints"`
	Context          map[string]any `json:"context"`
	RequestedPlan    *SkillPlan     `json:"requested_plan,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
}

type AnalysisSubmitRequest struct {
	DatasetName      *string        `json:"dataset_name,omitempty"`
	DatasetVersionID *string        `json:"dataset_version_id,omitempty"`
	DataType         *string        `json:"data_type,omitempty"`
	Goal             string         `json:"goal"`
	Constraints      []string       `json:"constraints"`
	Context          map[string]any `json:"context"`
	RequestedPlan    *SkillPlan     `json:"requested_plan,omitempty"`
}

type PlanRecord struct {
	PlanID               string    `json:"plan_id"`
	RequestID            string    `json:"request_id"`
	ProjectID            string    `json:"project_id"`
	DatasetName          string    `json:"dataset_name"`
	DatasetVersionID     *string   `json:"dataset_version_id,omitempty"`
	Plan                 SkillPlan `json:"plan"`
	Status               string    `json:"status"`
	PlannerType          *string   `json:"planner_type,omitempty"`
	PlannerModel         *string   `json:"planner_model,omitempty"`
	PlannerPromptVersion *string   `json:"planner_prompt_version,omitempty"`
	PlanHash             *string   `json:"plan_hash,omitempty"`
	CreatedAt            time.Time `json:"created_at"`
}

type AnalysisPlanResponse struct {
	Request AnalysisRequest `json:"request"`
	Plan    PlanRecord      `json:"plan"`
}

type AnalysisExecuteResponse struct {
	Request   AnalysisRequest  `json:"request"`
	Plan      PlanRecord       `json:"plan"`
	Execution ExecutionSummary `json:"execution"`
	JobID     *string          `json:"job_id,omitempty"`
}

type ExecutionEvent struct {
	ExecutionID string         `json:"execution_id"`
	TS          time.Time      `json:"ts"`
	Level       string         `json:"level"`
	EventType   string         `json:"event_type"`
	Message     string         `json:"message"`
	Payload     map[string]any `json:"payload,omitempty"`
}

type ExecutionSummary struct {
	ExecutionID              string                `json:"execution_id"`
	ProjectID                string                `json:"project_id"`
	RequestID                string                `json:"request_id"`
	Plan                     SkillPlan             `json:"plan"`
	Status                   string                `json:"status"`
	CreatedAt                time.Time             `json:"created_at"`
	EndedAt                  *time.Time            `json:"ended_at,omitempty"`
	RequiredHashes           []string              `json:"required_hashes"`
	EmbeddingModel           *string               `json:"embedding_model_version,omitempty"`
	Artifacts                map[string]string     `json:"artifacts"`
	DatasetVersionID         *string               `json:"dataset_version_id,omitempty"`
	CodeVersion              *string               `json:"code_version,omitempty"`
	ParamsHash               *string               `json:"params_hash,omitempty"`
	SkillBundleVersion       *string               `json:"skill_bundle_version,omitempty"`
	ProfileSnapshot          *DatasetProfile       `json:"profile_snapshot,omitempty"`
	Events                   []ExecutionEvent      `json:"events"`
	ResultV1Snapshot         *ExecutionResultV1    `json:"-"`
	FinalAnswerSnapshot      *ExecutionFinalAnswer `json:"-"`
	FinalAnswerPromptVersion *string               `json:"-"`
	FinalAnswerError         *string               `json:"-"`
	Diagnostics              *ExecutionDiagnostics `json:"diagnostics,omitempty"`
}

type ExecutionListItem struct {
	ExecutionID      string                 `json:"execution_id"`
	Status           string                 `json:"status"`
	CreatedAt        time.Time              `json:"created_at"`
	EndedAt          *time.Time             `json:"ended_at,omitempty"`
	DatasetVersionID *string                `json:"dataset_version_id,omitempty"`
	PrimarySkillName *string                `json:"primary_skill_name,omitempty"`
	AnswerPreview    *string                `json:"answer_preview,omitempty"`
	WarningCount     int                    `json:"warning_count"`
	Waiting          *ExecutionWaitingState `json:"waiting,omitempty"`
	Diagnostics      *ExecutionDiagnostics  `json:"diagnostics,omitempty"`
}

type ExecutionListResponse struct {
	Items []ExecutionListItem `json:"items"`
}

type PlanExecuteResponse struct {
	Plan      PlanRecord       `json:"plan"`
	Execution ExecutionSummary `json:"execution"`
	JobID     *string          `json:"job_id,omitempty"`
}

type ExecutionRerunRequest struct {
	Mode        *string `json:"mode,omitempty"`
	TriggeredBy *string `json:"triggered_by,omitempty"`
}

type ExecutionResumeRequest struct {
	Reason      *string `json:"reason,omitempty"`
	TriggeredBy *string `json:"triggered_by,omitempty"`
}

type ExecutionRerunResponse struct {
	Execution ExecutionSummary `json:"execution"`
	JobID     *string          `json:"job_id,omitempty"`
}

type ExecutionResultResponse struct {
	ExecutionID string                `json:"execution_id"`
	Artifacts   map[string]string     `json:"artifacts"`
	Contract    map[string]any        `json:"contract"`
	ResultV1    ExecutionResultV1     `json:"result_v1"`
	FinalAnswer *ExecutionFinalAnswer `json:"final_answer,omitempty"`
	Diagnostics *ExecutionDiagnostics `json:"diagnostics,omitempty"`
}

type ReportDraftCreateRequest struct {
	Title        *string  `json:"title,omitempty"`
	ExecutionIDs []string `json:"execution_ids"`
}

type ReportDraft struct {
	DraftID      string        `json:"draft_id"`
	ProjectID    string        `json:"project_id"`
	Title        string        `json:"title"`
	ExecutionIDs []string      `json:"execution_ids"`
	Content      ReportDraftV1 `json:"content"`
	CreatedAt    time.Time     `json:"created_at"`
}

type ReportDraftV1 struct {
	SchemaVersion     string               `json:"schema_version"`
	Title             string               `json:"title"`
	Overview          string               `json:"overview"`
	ExecutionCount    int                  `json:"execution_count"`
	Sections          []ReportDraftSection `json:"sections,omitempty"`
	KeyFindings       []string             `json:"key_findings,omitempty"`
	Evidence          []map[string]any     `json:"evidence,omitempty"`
	FollowUpQuestions []string             `json:"follow_up_questions,omitempty"`
	UsageSummary      map[string]any       `json:"usage_summary,omitempty"`
	Warnings          []string             `json:"warnings,omitempty"`
}

type ReportDraftSection struct {
	ExecutionID      string           `json:"execution_id"`
	Status           string           `json:"status"`
	CreatedAt        time.Time        `json:"created_at"`
	PrimarySkillName *string          `json:"primary_skill_name,omitempty"`
	Summary          string           `json:"summary"`
	KeyFindings      []string         `json:"key_findings,omitempty"`
	Evidence         []map[string]any `json:"evidence,omitempty"`
	WarningCount     int              `json:"warning_count"`
}

type ExecutionResultV1 struct {
	SchemaVersion      string                  `json:"schema_version"`
	Status             string                  `json:"status"`
	PrimaryArtifactKey *string                 `json:"primary_artifact_key,omitempty"`
	PrimarySkillName   *string                 `json:"primary_skill_name,omitempty"`
	Answer             *ExecutionResultAnswer  `json:"answer,omitempty"`
	StepResults        []ExecutionStepResultV1 `json:"step_results,omitempty"`
	UsageSummary       map[string]any          `json:"usage_summary,omitempty"`
	Profile            *DatasetProfile         `json:"profile,omitempty"`
	Warnings           []string                `json:"warnings,omitempty"`
	Waiting            *ExecutionWaitingState  `json:"waiting,omitempty"`
}

type ExecutionFinalAnswer struct {
	SchemaVersion     string           `json:"schema_version"`
	Status            string           `json:"status"`
	GenerationMode    string           `json:"generation_mode,omitempty"`
	Headline          string           `json:"headline,omitempty"`
	AnswerText        string           `json:"answer_text"`
	KeyPoints         []string         `json:"key_points,omitempty"`
	Caveats           []string         `json:"caveats,omitempty"`
	Evidence          []map[string]any `json:"evidence,omitempty"`
	FollowUpQuestions []string         `json:"follow_up_questions,omitempty"`
	PromptVersion     *string          `json:"prompt_version,omitempty"`
	Model             *string          `json:"model,omitempty"`
	Usage             map[string]any   `json:"usage,omitempty"`
	GeneratedAt       *time.Time       `json:"generated_at,omitempty"`
}

type ExecutionResultAnswer struct {
	Summary           string           `json:"summary"`
	KeyFindings       []string         `json:"key_findings,omitempty"`
	Evidence          []map[string]any `json:"evidence,omitempty"`
	FollowUpQuestions []string         `json:"follow_up_questions,omitempty"`
	SelectionSource   string           `json:"selection_source,omitempty"`
	CitationMode      string           `json:"citation_mode,omitempty"`
}

type ExecutionStepResultV1 struct {
	StepID        string         `json:"step_id"`
	SkillName     string         `json:"skill_name"`
	Status        string         `json:"status"`
	ArtifactKey   *string        `json:"artifact_key,omitempty"`
	Summary       string         `json:"summary,omitempty"`
	Usage         map[string]any `json:"usage,omitempty"`
	ArtifactRef   *string        `json:"artifact_ref,omitempty"`
	Warnings      []string       `json:"warnings,omitempty"`
	SelectionMode string         `json:"selection_mode,omitempty"`
}

type ExecutionWaitingState struct {
	WaitingFor string `json:"waiting_for"`
	Reason     string `json:"reason,omitempty"`
}

type ExecutionDiagnostics struct {
	EventCount         int                    `json:"event_count"`
	LatestEventType    string                 `json:"latest_event_type,omitempty"`
	LatestEventMessage string                 `json:"latest_event_message,omitempty"`
	FailureReason      string                 `json:"failure_reason,omitempty"`
	Waiting            *ExecutionWaitingState `json:"waiting,omitempty"`
	FinalAnswerStatus  string                 `json:"final_answer_status,omitempty"`
	FinalAnswerError   string                 `json:"final_answer_error,omitempty"`
}

type ExecutionDiffStep struct {
	StepID    string         `json:"step_id"`
	SkillName string         `json:"skill_name"`
	Status    string         `json:"status"`
	FromHash  *string        `json:"from_hash,omitempty"`
	ToHash    *string        `json:"to_hash,omitempty"`
	Stats     map[string]any `json:"stats,omitempty"`
}

type ExecutionDiffResponse struct {
	FromExecutionID string              `json:"from_execution_id"`
	ToExecutionID   string              `json:"to_execution_id"`
	TotalSteps      int                 `json:"total_steps"`
	ChangedSteps    int                 `json:"changed_steps"`
	Steps           []ExecutionDiffStep `json:"steps"`
}
