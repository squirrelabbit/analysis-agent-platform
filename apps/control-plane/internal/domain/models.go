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

type DatasetVersion struct {
	DatasetVersionID   string         `json:"dataset_version_id"`
	DatasetID          string         `json:"dataset_id"`
	ProjectID          string         `json:"project_id"`
	StorageURI         string         `json:"storage_uri"`
	DataType           string         `json:"data_type"`
	RecordCount        *int           `json:"record_count,omitempty"`
	Metadata           map[string]any `json:"metadata"`
	PrepareStatus      string         `json:"prepare_status"`
	PrepareModel       *string        `json:"prepare_model,omitempty"`
	PreparePromptVer   *string        `json:"prepare_prompt_version,omitempty"`
	PrepareURI         *string        `json:"prepare_uri,omitempty"`
	PreparedAt         *time.Time     `json:"prepared_at,omitempty"`
	SentimentStatus    string         `json:"sentiment_status"`
	SentimentModel     *string        `json:"sentiment_model,omitempty"`
	SentimentURI       *string        `json:"sentiment_uri,omitempty"`
	SentimentLabeledAt *time.Time     `json:"sentiment_labeled_at,omitempty"`
	SentimentPromptVer *string        `json:"sentiment_prompt_version,omitempty"`
	EmbeddingStatus    string         `json:"embedding_status"`
	EmbeddingModel     *string        `json:"embedding_model,omitempty"`
	EmbeddingURI       *string        `json:"embedding_uri,omitempty"`
	CreatedAt          time.Time      `json:"created_at"`
	ReadyAt            *time.Time     `json:"ready_at,omitempty"`
}

type DatasetVersionCreateRequest struct {
	StorageURI        string         `json:"storage_uri"`
	DataType          *string        `json:"data_type,omitempty"`
	RecordCount       *int           `json:"record_count,omitempty"`
	Metadata          map[string]any `json:"metadata,omitempty"`
	PrepareRequired   *bool          `json:"prepare_required,omitempty"`
	PrepareModel      *string        `json:"prepare_model,omitempty"`
	SentimentRequired *bool          `json:"sentiment_required,omitempty"`
	SentimentModel    *string        `json:"sentiment_model,omitempty"`
	EmbeddingRequired *bool          `json:"embedding_required,omitempty"`
	EmbeddingModel    *string        `json:"embedding_model,omitempty"`
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

type ExecutionEvent struct {
	ExecutionID string         `json:"execution_id"`
	TS          time.Time      `json:"ts"`
	Level       string         `json:"level"`
	EventType   string         `json:"event_type"`
	Message     string         `json:"message"`
	Payload     map[string]any `json:"payload,omitempty"`
}

type ExecutionSummary struct {
	ExecutionID        string             `json:"execution_id"`
	ProjectID          string             `json:"project_id"`
	RequestID          string             `json:"request_id"`
	Plan               SkillPlan          `json:"plan"`
	Status             string             `json:"status"`
	CreatedAt          time.Time          `json:"created_at"`
	EndedAt            *time.Time         `json:"ended_at,omitempty"`
	RequiredHashes     []string           `json:"required_hashes"`
	EmbeddingModel     *string            `json:"embedding_model_version,omitempty"`
	Artifacts          map[string]string  `json:"artifacts"`
	DatasetVersionID   *string            `json:"dataset_version_id,omitempty"`
	CodeVersion        *string            `json:"code_version,omitempty"`
	ParamsHash         *string            `json:"params_hash,omitempty"`
	SkillBundleVersion *string            `json:"skill_bundle_version,omitempty"`
	Events             []ExecutionEvent   `json:"events"`
	ResultV1Snapshot   *ExecutionResultV1 `json:"-"`
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
	ExecutionID string            `json:"execution_id"`
	Artifacts   map[string]string `json:"artifacts"`
	Contract    map[string]any    `json:"contract"`
	ResultV1    ExecutionResultV1 `json:"result_v1"`
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
	Warnings           []string                `json:"warnings,omitempty"`
	Waiting            *ExecutionWaitingState  `json:"waiting,omitempty"`
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
