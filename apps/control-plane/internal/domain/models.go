package domain

import (
	"encoding/json"
	"time"
)

type Project struct {
	ProjectID           string    `json:"project_id"`
	Name                string    `json:"name"`
	Description         *string   `json:"description,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	DatasetCount        int       `json:"dataset_count"`
	DatasetVersionCount int       `json:"dataset_version_count"`
	ScenarioCount       int       `json:"scenario_count"`
	PromptCount         int       `json:"prompt_count"`
	// silverone 2026-06-01 — project 단위 analysis thread 합산. 사이드바
	// 채팅 count 표시용. dataset 단위 thread API(ListAnalysisThreads)는 그대로
	// 유지. 프론트가 dataset별로 N+1 호출 안 하도록 service에서 단일 COUNT 합산.
	AnalysisThreadCount int `json:"analysis_thread_count"`
}

type ProjectCreateRequest struct {
	Name        string  `json:"name"`
	Description *string `json:"description,omitempty"`
}

type ProjectListResponse struct {
	Items []Project `json:"items"`
}

type ProjectPrompt struct {
	ProjectID   string    `json:"project_id"`
	Version     string    `json:"version"`
	Operation   string    `json:"operation"`
	Title       string    `json:"title"`
	Status      string    `json:"status"`
	Summary     string    `json:"summary,omitempty"`
	Content     string    `json:"content"`
	ContentHash string    `json:"content_hash"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type ProjectPromptUpsertRequest struct {
	Version   string `json:"version"`
	Operation string `json:"operation"`
	Content   string `json:"content"`
	// ChangeReason is required (ADR-015 §C2). Empty string ⇒ HTTP 400.
	// Records why this version was created/edited so audit history is
	// useful — silverone explicitly flagged that "변경이 진짜 많을
	// 것"이고 audit 부재가 운영자 가치 깎는다고 했음.
	ChangeReason string `json:"change_reason"`
	// CallerIsOperator is set by the HTTP handler when the request carries
	// the ``X-Operator-Mode: 1`` header (ADR-015 §D1 soft enforcement
	// until auth lands). Service rejects operator_only operations
	// (planner / planner_meta) when this is false. The field is JSON-
	// excluded so analysts cannot self-elevate via the body.
	CallerIsOperator bool `json:"-"`
}

type ProjectPromptListResponse struct {
	Items []ProjectPrompt `json:"items"`
}

// ProjectPromptChange is one event in the ADR-015 §C audit log for a
// per-project prompt. Created/updated/reverted actions all append a
// row; the table is append-only so history is preserved even after
// the prompt itself is deleted.
type ProjectPromptChange struct {
	ChangeID            string    `json:"change_id"`
	ProjectID           string    `json:"project_id"`
	Version             string    `json:"version"`
	Operation           string    `json:"operation"`
	Action              string    `json:"action"` // create / update / revert
	ChangeReason        string    `json:"change_reason"`
	ChangedBy           string    `json:"changed_by,omitempty"` // empty until auth lands
	PreviousContentHash string    `json:"previous_content_hash,omitempty"`
	NewContentHash      string    `json:"new_content_hash"`
	BaseVersion         string    `json:"base_version,omitempty"` // populated on action=revert
	ChangedAt           time.Time `json:"changed_at"`
}

type ProjectPromptHistoryResponse struct {
	Items []ProjectPromptChange `json:"items"`
}

type ProjectPromptRevertRequest struct {
	ToVersion    string `json:"to_version"`
	NewVersion   string `json:"new_version"`
	ChangeReason string `json:"change_reason"`
	// CallerIsOperator — see ProjectPromptUpsertRequest. Plumbed from
	// the X-Operator-Mode HTTP header by the handler.
	CallerIsOperator bool `json:"-"`
}

type ProjectPromptDiffResponse struct {
	ProjectID   string                 `json:"project_id"`
	Operation   string                 `json:"operation"`
	BaseVersion string                 `json:"base_version"`
	HeadVersion string                 `json:"head_version"`
	BaseContent string                 `json:"base_content"`
	HeadContent string                 `json:"head_content"`
	UnifiedDiff string                 `json:"unified_diff"`
	Stats       ProjectPromptDiffStats `json:"stats"`
}

type ProjectPromptDiffStats struct {
	AddedLines   int `json:"added_lines"`
	RemovedLines int `json:"removed_lines"`
	BaseLines    int `json:"base_lines"`
	HeadLines    int `json:"head_lines"`
}

// 5/6 화면기획서 B안 채택: 전역 prompt 도메인 타입 (Prompt / PromptCreateRequest /
// PromptUpdateRequest / PromptListResponse) 4개 제거. 글로벌 prompt는 .md 코드
// 계약. 프로젝트별 타입 (ProjectPrompt 등)은 그대로 유지.

type ProjectPromptDefaults struct {
	ProjectID                         string     `json:"project_id"`
	PreparePromptVersion              *string    `json:"prepare_prompt_version,omitempty"`
	SentimentPromptVersion            *string    `json:"sentiment_prompt_version,omitempty"`
	PlannerPromptVersion              *string    `json:"planner_prompt_version,omitempty"`
	PlannerMetaPromptVersion          *string    `json:"planner_meta_prompt_version,omitempty"`
	IssueEvidenceSummaryPromptVersion *string    `json:"issue_evidence_summary_prompt_version,omitempty"`
	ExecutionFinalAnswerPromptVersion *string    `json:"execution_final_answer_prompt_version,omitempty"`
	UpdatedAt                         *time.Time `json:"updated_at,omitempty"`
}

type ProjectPromptDefaultsUpdateRequest struct {
	PreparePromptVersion              *string `json:"prepare_prompt_version"`
	SentimentPromptVersion            *string `json:"sentiment_prompt_version"`
	PlannerPromptVersion              *string `json:"planner_prompt_version"`
	PlannerMetaPromptVersion          *string `json:"planner_meta_prompt_version"`
	IssueEvidenceSummaryPromptVersion *string `json:"issue_evidence_summary_prompt_version"`
	ExecutionFinalAnswerPromptVersion *string `json:"execution_final_answer_prompt_version"`
}

// δ-3 (5/21) — Scenario / ScenarioStep / ScenarioCreateRequest /
// ScenarioListResponse / ScenarioImportRow / ScenarioImportRequest /
// ScenarioImportResponse / ScenarioPlanCreateRequest 8 type 제거.
// 옛 1.x 시나리오 흐름은 analyze endpoint로 대체됨.

type Dataset struct {
	DatasetID              string     `json:"dataset_id"`
	ProjectID              string     `json:"project_id"`
	Name                   string     `json:"name"`
	Description            *string    `json:"description,omitempty"`
	DataType               string     `json:"data_type"`
	ActiveDatasetVersionID *string    `json:"active_dataset_version_id,omitempty"`
	ActiveVersionUpdatedAt *time.Time `json:"active_version_updated_at,omitempty"`
	// Metadata — dataset-level 설정. silverone 2026-05-22 (옵션 α) — subject 분류
	// 등 dataset 단위 설정을 dataset_version과 분리해 보존한다. 현재 정의된
	// keypath:
	//   - metadata.doc_genuineness — {subject_type, subject_name,
	//     subject_aliases, recruitment_keywords}. control plane이 build 시
	//     payload에 inject + version metadata에 applied snapshot 저장.
	// 다른 key는 forward-compat용으로 그대로 보존된다.
	Metadata  map[string]any `json:"metadata,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

type DatasetCreateRequest struct {
	Name        string         `json:"name"`
	Description *string        `json:"description,omitempty"`
	DataType    *string        `json:"data_type,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// DatasetMetadataPatchRequest — PATCH /projects/{pid}/datasets/{did}/metadata
// 의 request body. top-level key 단위 merge (같은 key는 overwrite, 다른 key는
// 보존). nested object는 *통째로* overwrite (deep merge X) — patch 시맨틱을
// 단순하게 유지.
type DatasetMetadataPatchRequest struct {
	Metadata map[string]any `json:"metadata"`
}

// DatasetInfoUpdateRequest — PATCH /projects/{pid}/datasets/{did} 본문.
// silverone 2026-06-05 — 데이터셋 이름/설명 수정. 지정된(non-nil) 필드만 반영.
// name은 trim 후 빈 문자열이면 거부. data_type은 기존 버전/빌드와 정합성
// 위험이 있어 이 endpoint에서 변경하지 않는다.
type DatasetInfoUpdateRequest struct {
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
}

type DatasetListResponse struct {
	Items []Dataset `json:"items"`
}

type AnalysisThread struct {
	ThreadID         string    `json:"thread_id"`
	ProjectID        string    `json:"project_id"`
	DatasetID        string    `json:"dataset_id"`
	DatasetVersionID string    `json:"dataset_version_id"`
	Title            string    `json:"title,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
	MessageCount     int       `json:"message_count,omitempty"`
	LastMessage      string    `json:"last_message,omitempty"`
}

type AnalysisMessage struct {
	MessageID      string         `json:"message_id"`
	ThreadID       string         `json:"thread_id"`
	ProjectID      string         `json:"project_id"`
	DatasetID      string         `json:"dataset_id"`
	Role           string         `json:"role"`
	Content        string         `json:"content"`
	ContextSummary map[string]any `json:"context_summary,omitempty"`
	RunID          *string        `json:"run_id,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	// silverone 2026-06-01 — thread detail history rendering용 lightweight
	// display projection. assistant message + 연결된 run의 result_json에
	// composer.display 있으면 GetAnalysisThread service가 채워서 응답.
	// DB 컬럼 X — pure response-time projection (full result_json은 그대로
	// run.result_json에 보존). user message에는 항상 nil.
	Display map[string]any `json:"display,omitempty"`
	// silverone 2026-06-01 — thread detail history에 분석 계획도 노출. Display와
	// 같은 패턴으로 run.result_json의 plan을 projectAnalyzePlan keep-set으로
	// 추출해 채운다 (plan_version + steps[].id/.skill/.params). user message에
	// 는 항상 nil. step status/duration_ms/row_count 같은 추가 메타는 후속 PR.
	Plan map[string]any `json:"plan,omitempty"`
}

type AnalysisRun struct {
	RunID            string          `json:"run_id"`
	ThreadID         string          `json:"thread_id"`
	ProjectID        string          `json:"project_id"`
	DatasetID        string          `json:"dataset_id"`
	DatasetVersionID string          `json:"dataset_version_id"`
	UserMessageID    string          `json:"user_message_id,omitempty"`
	RequestJSON      map[string]any  `json:"request_json,omitempty"`
	ResultJSON       json.RawMessage `json:"result_json,omitempty"`
	Status           string          `json:"status"`
	ErrorMessage     *string         `json:"error_message,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
	CompletedAt      *time.Time      `json:"completed_at,omitempty"`
}

// PlannerRejectionEvent — planner가 answerable=false로 거절한 질문의 적재 이벤트
// (silverone 2026-06-01, PR2). skill upgrade backlog 축적용. out_of_dataset_scope는
// 저장하지 않고, unsupported_skill / missing_data_or_artifact만 저장한다.
// MessageID(거절 응답 assistant message id)에 UNIQUE를 걸어 중복 적재를 막는다.
type PlannerRejectionEvent struct {
	EventID       string         `json:"event_id"`
	ProjectID     string         `json:"project_id"`
	DatasetID     string         `json:"dataset_id"`
	ThreadID      string         `json:"thread_id"`
	MessageID     string         `json:"message_id"`
	UserQuestion  string         `json:"user_question"`
	Reason        string         `json:"reason"`
	Message       string         `json:"message,omitempty"`
	CapabilityGap map[string]any `json:"capability_gap,omitempty"`
	CreatedAt     time.Time      `json:"created_at"`
}

// ReportSavedResult — 채팅 분석 결과를 보고서 보관함에 저장한 스냅샷
// (silverone 2026-06-10). 보고서 탭이 이 보관함에서 결과를 골라 블록으로 구성한다.
// run.result_json의 composer.display / plan / assistant_content를 저장 시점에
// 그대로 얼려서(snapshot) 보존한다 — 이후 같은 질문을 재실행하거나 thread를
// 지워도 보고서에 박제된 결과가 변하지 않게 하기 위함. Display / Plan은
// composer.display / plan과 동일 shape (frontend-safe keep-set projection).
type ReportSavedResult struct {
	ResultID         string         `json:"result_id"`
	ProjectID        string         `json:"project_id"`
	DatasetID        string         `json:"dataset_id"`
	DatasetVersionID string         `json:"dataset_version_id"`
	ThreadID         string         `json:"thread_id,omitempty"`
	RunID            string         `json:"run_id,omitempty"`
	SourceMessageID  string         `json:"source_message_id,omitempty"`
	Title            string         `json:"title"`
	Question         string         `json:"question,omitempty"`
	AssistantContent string         `json:"assistant_content,omitempty"`
	Display          map[string]any `json:"display,omitempty"`
	Plan             map[string]any `json:"plan,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
}

// ReportSavedResultCreateRequest — 보관함 저장 요청. run_id로 어떤 분석 결과를
// 스냅샷할지 지정한다. thread_id는 scope·검증용. title은 미지정 시 서버가
// display.title 또는 question에서 유도한다.
type ReportSavedResultCreateRequest struct {
	ThreadID string `json:"thread_id"`
	RunID    string `json:"run_id"`
	Title    string `json:"title,omitempty"`
}

type ReportSavedResultListResponse struct {
	Items []ReportSavedResult `json:"items"`
}

// Report — 보고서 문서 (silverone 2026-06-11). saved_results(분석 결과 보관함)를
// 조합해 만든 별도 문서. blocks는 작성 당시 상태로 고정하기 위해 각 블록 안에
// snapshot(display/summary/rows)을 복제해 담는다 — source_result_id는 provenance
// 로만 보관하므로 원본 saved_result가 삭제/변경돼도 보고서는 깨지지 않는다.
//
// Blocks는 control-plane이 구조를 강제하지 않는 opaque JSON 배열이다. 블록
// contract(type/title/source_*/snapshot)는 프론트(보고서 에디터)가 소유하고
// 백엔드는 영속만 책임진다 — analysis_runs.result_json과 같은 정책. 1차는
// CRUD만 닫고 공유/재생성/export는 후속.
type Report struct {
	ReportID  string          `json:"report_id"`
	ProjectID string          `json:"project_id"`
	Title     string          `json:"title"`
	Blocks    json.RawMessage `json:"blocks"`
	CreatedAt time.Time       `json:"created_at"`
	UpdatedAt time.Time       `json:"updated_at"`
}

// ReportSummary — 목록용 경량 projection. blocks 본문 대신 개수만(목록이
// 무거워지지 않게). analysis_thread 목록과 같은 패턴.
type ReportSummary struct {
	ReportID   string    `json:"report_id"`
	ProjectID  string    `json:"project_id"`
	Title      string    `json:"title"`
	BlockCount int       `json:"block_count"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// ReportCreateRequest / ReportUpdateRequest — blocks 미지정 시 빈 배열로 저장.
type ReportCreateRequest struct {
	Title  string          `json:"title"`
	Blocks json.RawMessage `json:"blocks,omitempty"`
}

type ReportUpdateRequest struct {
	Title  string          `json:"title"`
	Blocks json.RawMessage `json:"blocks,omitempty"`
}

type ReportListResponse struct {
	Items []ReportSummary `json:"items"`
}

// DocGenuinenessOverride — 전처리 진성 분석 화면에서 운영자가 수동으로 보정한
// 진성 라벨 (silverone 2026-06-11). artifact JSONL은 LLOA 원본 그대로 두고
// 보정값을 별도 overlay로 저장한다 — 진성 분석 GET이 effective label로 합성하고
// 감사/재현/재실행 추적이 가능하게 한다. OriginalGenuineness는 보정 시점의
// artifact 라벨 snapshot(summary effective 재집계의 기준). version 스코프.
type DocGenuinenessOverride struct {
	ProjectID           string    `json:"project_id"`
	DatasetID           string    `json:"dataset_id"`
	DatasetVersionID    string    `json:"dataset_version_id"`
	DocID               string    `json:"doc_id"`
	OriginalGenuineness string    `json:"original_genuineness"`
	OriginalReason      string    `json:"original_reason,omitempty"`
	OverrideGenuineness string    `json:"override_genuineness"`
	OverrideReason      string    `json:"override_reason,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

// DocGenuinenessOverrideRequest — 보정 요청. genuineness는 effective로 적용할
// 라벨. reason은 선택(운영 메모).
type DocGenuinenessOverrideRequest struct {
	Genuineness string `json:"genuineness"`
	Reason      string `json:"reason,omitempty"`
}

// ClauseLabelOverride — 전처리 절 라벨링 화면에서 운영자가 절(clause)의 aspect/
// sentiment를 수동 보정한 overlay (silverone 2026-06-11). artifact JSONL(LLOA
// 원본)은 건드리지 않고 보정값만 저장하고, 절 라벨링 GET이 effective aspect/
// sentiment로 합성한다. original_*는 보정 시점 artifact 값 snapshot. clause_id는
// 뷰가 doc_id+절 index로 합성하는 값(version 스코프).
type ClauseLabelOverride struct {
	ProjectID         string    `json:"project_id"`
	DatasetID         string    `json:"dataset_id"`
	DatasetVersionID  string    `json:"dataset_version_id"`
	ClauseID          string    `json:"clause_id"`
	OriginalAspect    string    `json:"original_aspect"`
	OriginalSentiment string    `json:"original_sentiment"`
	OverrideAspect    string    `json:"override_aspect"`
	OverrideSentiment string    `json:"override_sentiment"`
	OverrideReason    string    `json:"override_reason,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// ClauseLabelOverrideRequest — aspect/sentiment를 effective로 적용. reason은 선택
// (비면 서버가 "운영자 수동 수정" 기본값). 둘 중 하나만 보내도 다른 쪽은 원본 유지.
type ClauseLabelOverrideRequest struct {
	Aspect    string `json:"aspect,omitempty"`
	Sentiment string `json:"sentiment,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type AnalysisThreadCreateRequest struct {
	Title string `json:"title,omitempty"`
}

type AnalysisThreadListResponse struct {
	Items []AnalysisThread `json:"items"`
}

type AnalysisThreadDetail struct {
	AnalysisThread
	Messages []AnalysisMessage `json:"messages"`
}

type AnalysisThreadMessageRequest struct {
	Content string `json:"content"`
}

type AnalysisThreadMessageResponse struct {
	ProjectID        string              `json:"project_id"`
	DatasetID        string              `json:"dataset_id"`
	ThreadID         string              `json:"thread_id"`
	DatasetVersionID string              `json:"dataset_version_id"`
	UserMessage      AnalysisMessageView `json:"user_message"`
	AssistantMessage AnalysisMessageView `json:"assistant_message,omitempty"`
	Run              AnalysisRunView     `json:"run"`
	Mode             string              `json:"mode"`
	Result           json.RawMessage     `json:"result,omitempty"`
}

// AnalysisMessageView — silverone 2026-05-28 frontend-safe projection. context_summary는
// 응답에서 제외 (DB에는 AnalysisMessage 원본 그대로 보존). user_message는
// run_id가 없으므로 omitempty로 자연 누락.
type AnalysisMessageView struct {
	MessageID string    `json:"message_id"`
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	RunID     *string   `json:"run_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// AnalysisRunView — silverone 2026-05-28 frontend-safe projection. request_json /
// result_json은 응답에서 제외 (raw payload·top-level result와 중복). project_id /
// dataset_id도 top-level과 중복이라 제외. thread_id / dataset_version_id /
// user_message_id는 식별자성으로 유지.
type AnalysisRunView struct {
	RunID            string     `json:"run_id"`
	ThreadID         string     `json:"thread_id"`
	DatasetVersionID string     `json:"dataset_version_id"`
	UserMessageID    string     `json:"user_message_id,omitempty"`
	Status           string     `json:"status"`
	ErrorMessage     *string    `json:"error_message,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	CompletedAt      *time.Time `json:"completed_at,omitempty"`
}

// ToView — AnalysisMessage(DB 원본)를 frontend-safe view로 projection.
// silverone 2026-05-28.
func (m AnalysisMessage) ToView() AnalysisMessageView {
	view := AnalysisMessageView{
		MessageID: m.MessageID,
		Role:      m.Role,
		Content:   m.Content,
		CreatedAt: m.CreatedAt,
	}
	if m.RunID != nil {
		runID := *m.RunID
		view.RunID = &runID
	}
	return view
}

// ToView — AnalysisRun(DB 원본)을 frontend-safe view로 projection.
// silverone 2026-05-28.
func (r AnalysisRun) ToView() AnalysisRunView {
	view := AnalysisRunView{
		RunID:            r.RunID,
		ThreadID:         r.ThreadID,
		DatasetVersionID: r.DatasetVersionID,
		UserMessageID:    r.UserMessageID,
		Status:           r.Status,
		CreatedAt:        r.CreatedAt,
	}
	if r.ErrorMessage != nil {
		msg := *r.ErrorMessage
		view.ErrorMessage = &msg
	}
	if r.CompletedAt != nil {
		completedAt := *r.CompletedAt
		view.CompletedAt = &completedAt
	}
	return view
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
	DatasetVersionID string                         `json:"dataset_version_id"`
	DatasetID        string                         `json:"dataset_id"`
	ProjectID        string                         `json:"project_id"`
	StorageURI       string                         `json:"storage_uri"`
	DataType         string                         `json:"data_type"`
	RecordCount      *int                           `json:"record_count,omitempty"`
	Metadata         map[string]any                 `json:"metadata"`
	SourceSummary    *DatasetSourceSummary          `json:"source_summary,omitempty"`
	BuildJobs        []DatasetVersionBuildJobStatus `json:"build_jobs,omitempty"`
	BuildStages      []DatasetVersionBuildStage     `json:"build_stages,omitempty"`
	Artifacts        []DatasetVersionArtifact       `json:"artifacts,omitempty"`
	Profile          *DatasetProfile                `json:"profile,omitempty"`
	CleanStatus      string                         `json:"clean_status"`
	CleanURI         *string                        `json:"clean_uri,omitempty"`
	CleanedRef       *string                        `json:"cleaned_ref,omitempty"`
	CleanedAt        *time.Time                     `json:"cleaned_at,omitempty"`
	CleanSummary     *DatasetCleanSummary           `json:"clean_summary,omitempty"`
	// silverone 2026-05-28 (β2 cleanup PR2) — prepare/sentiment/embedding 15
	// 필드 제거. ADR-018 β2로 단계 자체가 사라졌고 row에 NULL/default만 채워져
	// 있었음. DB 컬럼은 그대로 두고(PR5 마이그레이션) Go side read/write만 정리.
	IsActive  bool       `json:"is_active"`
	CreatedAt time.Time  `json:"created_at"`
	ReadyAt   *time.Time `json:"ready_at,omitempty"`
}

type DatasetVersionArtifact struct {
	ArtifactID       string         `json:"artifact_id"`
	ProjectID        string         `json:"project_id"`
	DatasetID        string         `json:"dataset_id"`
	DatasetVersionID string         `json:"dataset_version_id"`
	ArtifactType     string         `json:"artifact_type"`
	Stage            string         `json:"stage"`
	Status           string         `json:"status"`
	URI              string         `json:"uri,omitempty"`
	Format           string         `json:"format,omitempty"`
	Model            string         `json:"model,omitempty"`
	PromptVersion    string         `json:"prompt_version,omitempty"`
	Summary          map[string]any `json:"summary,omitempty"`
	Metadata         map[string]any `json:"metadata,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

type DatasetSourceColumnSummary struct {
	Name string `json:"name"`
	Type string `json:"type,omitempty"`
}

type DatasetSourceSummary struct {
	Available    bool                         `json:"available"`
	Status       string                       `json:"status,omitempty"`
	Format       string                       `json:"format,omitempty"`
	RowCount     *int                         `json:"row_count,omitempty"`
	ColumnCount  int                          `json:"column_count,omitempty"`
	Columns      []DatasetSourceColumnSummary `json:"columns,omitempty"`
	SampleLimit  int                          `json:"sample_limit,omitempty"`
	SampleRows   []map[string]any             `json:"sample_rows,omitempty"`
	ErrorMessage string                       `json:"error_message,omitempty"`
}

type DatasetVersionBuildJobStatus struct {
	JobID        string               `json:"job_id"`
	BuildType    string               `json:"build_type"`
	Status       string               `json:"status"`
	TriggeredBy  string               `json:"triggered_by,omitempty"`
	Attempt      int                  `json:"attempt"`
	CreatedAt    time.Time            `json:"created_at"`
	StartedAt    *time.Time           `json:"started_at,omitempty"`
	CompletedAt  *time.Time           `json:"completed_at,omitempty"`
	ErrorMessage *string              `json:"error_message,omitempty"`
	Diagnostics  *BuildJobDiagnostics `json:"diagnostics,omitempty"`
}

type DatasetVersionBuildStage struct {
	Stage           string                        `json:"stage"`
	Status          string                        `json:"status"`
	Applicable      bool                          `json:"applicable"`
	Required        bool                          `json:"required"`
	Ready           bool                          `json:"ready"`
	DependsOn       []string                      `json:"depends_on"`
	CanRun          bool                          `json:"can_run"`
	RunGroup        string                        `json:"run_group"`
	AutoRunEligible bool                          `json:"auto_run_eligible"`
	BlockedReason   *string                       `json:"blocked_reason,omitempty"`
	LatestJob       *DatasetVersionBuildJobStatus `json:"latest_job,omitempty"`
	PrimaryArtifact *DatasetVersionArtifact       `json:"primary_artifact,omitempty"`
	Artifacts       []DatasetVersionArtifact      `json:"artifacts,omitempty"`
	Summary         map[string]any                `json:"summary,omitempty"`
	Model           string                        `json:"model,omitempty"`
	PromptVersion   string                        `json:"prompt_version,omitempty"`
	ErrorMessage    *string                       `json:"error_message,omitempty"`
	Diagnostics     *BuildJobDiagnostics          `json:"diagnostics,omitempty"`
}

type DatasetVersionCreateRequest struct {
	StorageURI        string          `json:"storage_uri"`
	DataType          *string         `json:"data_type,omitempty"`
	RecordCount       *int            `json:"record_count,omitempty"`
	Metadata          map[string]any  `json:"metadata,omitempty"`
	Profile           *DatasetProfile `json:"profile,omitempty"`
	ActivateOnCreate  *bool           `json:"activate_on_create,omitempty"`
	PrepareRequired   *bool           `json:"prepare_required,omitempty"`
	PrepareLLMMode    *string         `json:"prepare_llm_mode,omitempty"`
	PrepareModel      *string         `json:"prepare_model,omitempty"`
	SentimentRequired *bool           `json:"sentiment_required,omitempty"`
	SentimentLLMMode  *string         `json:"sentiment_llm_mode,omitempty"`
	SentimentModel    *string         `json:"sentiment_model,omitempty"`
	EmbeddingRequired *bool           `json:"embedding_required,omitempty"`
	EmbeddingModel    *string         `json:"embedding_model,omitempty"`
}

type DatasetVersionListResponse struct {
	Items []DatasetVersionListItem `json:"items"`
}

// DatasetVersionDetail — GET /versions/{version_id} 응답. 운영자가 한 version을
// 열어 "각 단계 결과 + 파일 형태"만 보도록 압축한다. 내부 URI/artifacts/
// build_jobs/profile 등은 노출하지 않는다 — 실행 이력은 별도 endpoint.
type DatasetVersionDetail struct {
	DatasetVersionID string                    `json:"dataset_version_id"`
	VersionNumber    int                       `json:"version_number"`
	CreatedAt        time.Time                 `json:"created_at"`
	ReadyAt          *time.Time                `json:"ready_at,omitempty"`
	IsActive         bool                      `json:"is_active"`
	RowCount         int                       `json:"row_count"`
	ColumnCount      int                       `json:"column_count"`
	Columns          []string                  `json:"columns"`
	ByteSize         int64                     `json:"byte_size"`
	Clean            DatasetVersionStageDetail `json:"clean"`
	DocGenuineness   DatasetVersionStageDetail `json:"doc_genuineness"`
	ClauseLabel      DatasetVersionStageDetail `json:"clause_label"`
}

type DatasetVersionStageDetail struct {
	Status      string     `json:"status"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	Summary     any        `json:"summary,omitempty"`
}

// DatasetVersionListItem — version 목록에서 운영자가 한눈에 보고 다음 호출을
// 결정하는 데 필요한 최소 필드만 노출한다. 상세 메타·artifacts·build_jobs는
// GET /versions/{version_id}로 조회.
type DatasetVersionListItem struct {
	DatasetVersionID     string    `json:"dataset_version_id"`
	VersionNumber        int       `json:"version_number"`
	CreatedAt            time.Time `json:"created_at"`
	IsActive             bool      `json:"is_active"`
	RowCount             int       `json:"row_count"`
	ColumnCount          int       `json:"column_count"`
	Columns              []string  `json:"columns"`
	ByteSize             int64     `json:"byte_size"`
	CleanStatus          string    `json:"clean_status"`
	DocGenuinenessStatus string    `json:"doc_genuineness_status"`
	ClauseLabelStatus    string    `json:"clause_label_status"`
	OriginalFilename     string    `json:"original_filename"`
}

type DatasetActiveVersionUpdateRequest struct {
	DatasetVersionID string `json:"dataset_version_id"`
}

type DatasetCleanSummary struct {
	InputRowCount         int            `json:"input_row_count"`
	OutputRowCount        int            `json:"output_row_count"`
	KeptCount             int            `json:"kept_count"`
	DroppedCount          int            `json:"dropped_count"`
	SkippedRowCount       int            `json:"skipped_row_count,omitempty"`
	TextColumn            string         `json:"text_column,omitempty"`
	TextColumns           []string       `json:"text_columns,omitempty"`
	TextJoiner            string         `json:"text_joiner,omitempty"`
	SourceInputCharCount  int            `json:"source_input_char_count,omitempty"`
	CleanedInputCharCount int            `json:"cleaned_input_char_count,omitempty"`
	CleanReducedCharCount int            `json:"clean_reduced_char_count,omitempty"`
	CleanRegexRuleHits    map[string]int `json:"clean_regex_rule_hits,omitempty"`
}

// silverone 2026-05-28 (β2 cleanup PR2) — DatasetPrepareSummary /
// DatasetPrepareSample / DatasetSentimentSummary / DatasetSentimentSample /
// DatasetTableColumn 5 type 제거. ADR-018 β2로 prepare/sentiment build 단계가
// 사라져 사용처 없음.

// 2026-05-21 — output_path / force 두 필드 제거. text_columns만 유지.
// output_path는 운영자가 디스크 경로를 직접 결정할 일이 없어 내부 derive
// (`s.deriveCleanURI(version)`)만 쓰면 충분. force는 같은 dataset_version
// 재정제 시나리오 자체가 흔치 않고, 필요해지면 새 dataset_version으로 다시
// upload하는 게 운영상 더 안전.
// silverone 2026-05-28 (clean 정식화) — date_column optional 추가.
// 명시되면 해당 source 컬럼을 created_at ISO 8601 string으로 변환.
// parse 실패 / Invalid date / 빈 값은 null. 없으면 created_at 컬럼 자체
// 빈 column으로 출력 (executor가 NULL 허용).
type DatasetCleanRequest struct {
	TextColumns []string `json:"text_columns,omitempty"`
	DateColumn  *string  `json:"date_column,omitempty"`
}

// silverone 2026-05-28 (β2 cleanup PR2) — DatasetPrepareSampleResponse /
// DatasetSentimentSampleRequest / DatasetSentimentSampleResponse 3 DTO 제거.
// β2로 sample endpoint 자체가 사라져 사용처 0.

// DatasetClusterBuildRequest는 retrieve layer plan skill (embedding_cluster)와
// `cluster_materialization.go`가 plan-driven cluster 파라미터를 정규화하는 용도로
// 계속 사용한다. dataset_build로서의 "cluster" task 자체는 (β2) 결정으로 제거.
type DatasetClusterBuildRequest struct {
	EmbeddingIndexSourceRef *string  `json:"embedding_index_source_ref,omitempty"`
	ChunkRef                *string  `json:"chunk_ref,omitempty"`
	OutputPath              *string  `json:"output_path,omitempty"`
	SimilarityThreshold     *float64 `json:"similarity_threshold,omitempty"`
	TopN                    *int     `json:"top_n,omitempty"`
	SampleN                 *int     `json:"sample_n,omitempty"`
	Force                   *bool    `json:"force,omitempty"`
}

// ADR-017 / 5/19 결정 — clean 직후 doc-level 3-tier 진성 분류 build request.
// 5/20 결정 — request body 단순화. output_path / max_tokens / batch_size /
// aspect_taxonomy_version 같은 worker 내부 default가 derive하는 필드는 제거.
type DatasetDocGenuinenessBuildRequest struct {
	DocGenuinenessPromptVer *string `json:"doc_genuineness_prompt_version,omitempty"`
	// silverone 2026-06-12 — 전처리 LLOA 모델 선택. 생략 시 worker env(LLOA_MODEL)
	// default. allowlist(LLOA_MODELS) 검증은 job 생성 시 control-plane이 수행.
	ModelID *string `json:"model_id,omitempty"`
	// silverone 2026-06-15 (ADR-026) — verify 모드. true면 ClassifyModels 2개로
	// 교차 분류 + 불일치 시 JudgeModel이 judge. final_label/needs_review 산출.
	// ModelID(단일)와 동시 사용 안 함.
	Verify         *bool    `json:"verify,omitempty"`
	ClassifyModels []string `json:"classify_models,omitempty"`
	JudgeModel     *string  `json:"judge_model,omitempty"`
	Force          *bool    `json:"force,omitempty"`
}

type DatasetClauseLabelBuildRequest struct {
	ClauseLabelPromptVer *string `json:"clause_label_prompt_version,omitempty"`
	// 5/20 결정 — doc_genuineness 결과로 필터링. nil이면 default
	// ["genuine_review", "mixed"]로 자동 ON. explicit empty list ``[]``로 opt-out.
	IncludeGenuineness []string `json:"include_genuineness,omitempty"`
	// silverone 2026-06-12 — 전처리 LLOA 모델 선택 (doc_genuineness와 동일 정책).
	ModelID *string `json:"model_id,omitempty"`
	Force   *bool   `json:"force,omitempty"`
}

// LLOAModelOption — 전처리(doc_genuineness/clause_label) 빌드에 선택 가능한
// LLOA 모델 항목. LLOA_MODELS env에서 파싱되며 Default는 LLOA_MODEL(worker
// default)과 일치하는 항목에 표시된다.
type LLOAModelOption struct {
	ModelID string `json:"model_id"`
	Label   string `json:"label"`
	Default bool   `json:"default"`
}

// silverone 2026-06-10 — 수동 keyword build 요청. clause_label_ref 존재가 precondition.
// LLOA 호출 없는 결정론적 Kiwi 추출이라 입력 최소(선택 keyword_min_len).
type DatasetClauseKeywordsBuildRequest struct {
	KeywordMinLen *int `json:"keyword_min_len,omitempty"`
}

type DatasetBuildJob struct {
	JobID            string         `json:"job_id"`
	ProjectID        string         `json:"project_id"`
	DatasetID        string         `json:"dataset_id"`
	DatasetVersionID string         `json:"dataset_version_id"`
	BuildType        string         `json:"build_type"`
	Status           string         `json:"status"`
	Request          map[string]any `json:"request,omitempty"`
	TriggeredBy      string         `json:"triggered_by,omitempty"`
	WorkflowID       *string        `json:"workflow_id,omitempty"`
	WorkflowRunID    *string        `json:"workflow_run_id,omitempty"`
	Attempt          int            `json:"attempt"`
	LastErrorType    *string        `json:"last_error_type,omitempty"`
	// 2026-05-21 — ResumedExecutionCount 제거. δ-3에서 executions 테이블 drop
	// 으로 증가시킬 path 자체가 사라졌다. 항상 0이라 응답 noise.
	CreatedAt    time.Time            `json:"created_at"`
	StartedAt    *time.Time           `json:"started_at,omitempty"`
	CompletedAt  *time.Time           `json:"completed_at,omitempty"`
	ErrorMessage *string              `json:"error_message,omitempty"`
	Diagnostics  *BuildJobDiagnostics `json:"diagnostics,omitempty"`
}

type DatasetBuildJobListResponse struct {
	Items []DatasetBuildJob `json:"items"`
}

// DatasetBuildJobAccepted — POST /clean / /doc_genuineness / /clause_label
// 응답 slim shape. 2026-05-21 결정: project_id/dataset_id/dataset_version_id는
// URL path에 이미 있고 attempt/triggered_by/workflow_*/diagnostics 같은 상세
// 필드는 GET /dataset_build_jobs/{job_id} 또는 /versions/{version_id}/build_jobs
// 로 위임. POST 응답은 "큐에 들어갔다" 알림 + polling key + 상태 4 필드만.
type DatasetBuildJobAccepted struct {
	JobID     string    `json:"job_id"`
	BuildType string    `json:"build_type"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// AsAccepted — DatasetBuildJob → slim accepted 응답으로 변환.
func (j DatasetBuildJob) AsAccepted() DatasetBuildJobAccepted {
	return DatasetBuildJobAccepted{
		JobID:     j.JobID,
		BuildType: j.BuildType,
		Status:    j.Status,
		CreatedAt: j.CreatedAt,
	}
}

type BuildJobDiagnostics struct {
	RetryCount        int               `json:"retry_count"`
	LastErrorType     *string           `json:"last_error_type,omitempty"`
	LastErrorMessage  *string           `json:"last_error_message,omitempty"`
	WorkflowID        *string           `json:"workflow_id,omitempty"`
	WorkflowRunID     *string           `json:"workflow_run_id,omitempty"`
	Progress          *BuildJobProgress `json:"progress,omitempty"`
	LLMFallback       bool              `json:"llm_fallback,omitempty"`
	LLMFallbackReason *string           `json:"llm_fallback_reason,omitempty"`
	LLMFallbackCount  int               `json:"llm_fallback_count,omitempty"`
	LLMProvider       *string           `json:"llm_provider,omitempty"`
	LLMModel          *string           `json:"llm_model,omitempty"`
	Warnings          []string          `json:"warnings,omitempty"`
}

// DatasetArtifactView — 화면 polling용 GET 응답.
// /versions/{vid}/clean, /doc_genuineness, /clause_label 3 endpoint가 공유.
// 화면은 이 view만 polling하면 build job 진행/실패/완료를 단일 진입점으로
// 추적할 수 있다 (/dataset_build_jobs/{job_id} 직접 호출 불필요).
//
// status enum:
//   - not_started: artifact 없음 + 같은 build_type의 job 없음
//   - queued / running / failed: 최근 같은 build_type job의 status
//   - completed: artifact ref가 ready이고 최근 job이 completed
//
// 공통 필드(status/job_id/started_at/completed_at/error_message/progress)는
// 모든 build_type에서 채워진다. summary/items/pagination은 단계별로 다르고,
// clean은 status="completed"일 때 summary만, doc_genuineness/clause_label은
// summary + items + pagination + applied를 모두 채운다.
type DatasetArtifactView struct {
	BuildType   string     `json:"build_type"`
	Status      string     `json:"status"`
	JobID       *string    `json:"job_id"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
	// DurationSeconds — 작업 경과 시간(초). silverone 2026-05-26.
	//   - completed/failed (completed_at != nil): completed_at - started_at (확정값)
	//   - queued/running (completed_at == nil): now - started_at (진행 중 실시간)
	//   - started_at == nil → null
	// 화면이 started_at/completed_at를 직접 계산하지 않아도 되게 서버가 채운다.
	DurationSeconds *float64            `json:"duration_seconds"`
	ErrorMessage    *string             `json:"error_message"`
	Progress        *ArtifactProgress   `json:"progress,omitempty"`
	Applied         map[string]any      `json:"applied,omitempty"`
	Summary         map[string]any      `json:"summary,omitempty"`
	Items           []map[string]any    `json:"items,omitempty"`
	Pagination      *ArtifactPagination `json:"pagination,omitempty"`
}

// ArtifactProgress — build job 진행률. worker가 progress 파일에 percent /
// processed_rows / total_rows를 기록하면 service가 읽어서 화면에 노출.
type ArtifactProgress struct {
	Percent       float64    `json:"percent"`
	ProcessedRows int        `json:"processed_rows,omitempty"`
	TotalRows     int        `json:"total_rows,omitempty"`
	ETASeconds    *float64   `json:"eta_seconds,omitempty"`
	Message       string     `json:"message,omitempty"`
	UpdatedAt     *time.Time `json:"updated_at,omitempty"`
}

// ArtifactPagination — DatasetArtifactView pagination 정보. doc_genuineness /
// clause_label view에서만 사용. clean view에는 items가 없어서 미사용.
type ArtifactPagination struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
	Total  int `json:"total"`
}

// ── doc_genuineness 모델 비교 (silverone 2026-06-15) ──
// 같은 원본을 두 모델로 빌드한 두 버전의 진성 분류 결과를 doc_id 기준으로
// 1:1 비교한다. 비교값은 override 적용 전 *원본 모델 라벨*이다(override는 사람
// 보정이라 모델 간 비교를 오염시키므로 제외하고, 정답 힌트로만 노출).

// DocGenuinenessRun — 한 버전에 보관된 모델별 진성 분류 결과 1건 (silverone
// 2026-06-15). 같은 버전을 다른 모델로 재실행하면 덮어쓰지 않고 모델별로
// 누적되며, 비교는 한 버전 안의 두 run(모델) 사이에서 이뤄진다.
type DocGenuinenessRun struct {
	Model            string    `json:"model"`
	ModelDisplayName string    `json:"model_display_name,omitempty"` // 응답 시점 env 기반
	Ref              string    `json:"ref"`                          // 이 모델 결과 artifact 경로
	PromptVersion    string    `json:"prompt_version,omitempty"`
	CompletedAt      time.Time `json:"completed_at"`
}

// DocGenuinenessRunsResponse — GET .../doc_genuineness/runs 응답.
type DocGenuinenessRunsResponse struct {
	DatasetVersionID string              `json:"dataset_version_id"`
	Items            []DocGenuinenessRun `json:"items"`
}

// DocGenuinenessCompareSide — 비교 한쪽 메타. version_a/version_b는 같은 버전이고
// model로 구분된다.
type DocGenuinenessCompareSide struct {
	DatasetVersionID string `json:"dataset_version_id"`
	Model            string `json:"model,omitempty"`             // 이 run의 모델 id
	ModelDisplayName string `json:"model_display_name,omitempty"` // env 기반 표시명
	Total            int    `json:"total"`                       // 이 run의 doc 수
}

// DocGenuinenessCompareDisagreement — 두 모델이 다르게 분류한 문서 1건.
type DocGenuinenessCompareDisagreement struct {
	DocID               string `json:"doc_id"`
	AGenuineness        string `json:"a_genuineness"`
	AReason             string `json:"a_reason,omitempty"`
	BGenuineness        string `json:"b_genuineness"`
	BReason             string `json:"b_reason,omitempty"`
	CleanedText         string `json:"cleaned_text,omitempty"`
	OverrideGenuineness string `json:"override_genuineness,omitempty"` // 사람 보정(정답 힌트), 있으면
}

// DocGenuinenessCompareView — 비교 리포트 응답.
type DocGenuinenessCompareView struct {
	VersionA DocGenuinenessCompareSide `json:"version_a"`
	VersionB DocGenuinenessCompareSide `json:"version_b"`
	Tiers    []string                  `json:"tiers"` // confusion 행/열 순서
	// Compared — 양쪽에 모두 존재하는 doc 수. Matched — 그중 라벨 일치 수.
	Compared int     `json:"compared"`
	Matched  int     `json:"matched"`
	Rate     float64 `json:"agreement_rate"` // matched/compared, compared=0이면 0
	OnlyInA  int     `json:"only_in_a"`      // 한쪽에만 있는 doc(소스 불일치 신호)
	OnlyInB  int     `json:"only_in_b"`
	// Confusion — A 라벨(행) × B 라벨(열) 카운트. tiers 순서.
	Confusion [][]int `json:"confusion"`
	// Disagreements — 불일치 문서. pagination은 이 목록에만 적용.
	Disagreements      []DocGenuinenessCompareDisagreement `json:"disagreements"`
	DisagreementsTotal int                                 `json:"disagreements_total"`
	Pagination         *ArtifactPagination                 `json:"pagination,omitempty"`

	// ── 결론 레이어 (silverone 2026-06-15) — 정답 판정이 아니라 합의/불일치
	// 기반 판정 보조. ──
	// Patterns — 불일치 패턴(A 라벨→B 라벨) 빈도 내림차순. "어디서 주로 갈리나".
	Patterns []DocGenuinenessComparePattern `json:"patterns"`
	// OverrideEval — 사람 보정(정답)이 있는 문서 기준 모델별 정확도. 정답 샘플이
	// 없으면 nil(=판정 불가, agreement만).
	OverrideEval *DocGenuinenessOverrideEval `json:"override_eval,omitempty"`
	// UnreviewedDisagreements — 정답 보정이 아직 없는 불일치 수(우선 검토 대상).
	UnreviewedDisagreements int `json:"unreviewed_disagreements"`
	// VerdictLevel — 자동 결론의 신뢰 수준.
	//   ground_truth   — 정답 샘플 있음 → 모델별 정확도로 우열 제시 가능
	//   agreement_only — 정답 없음 + 일치율 높음 → 일치율만, 우열 판단 불가
	//   review_needed  — 일치율 낮음 → 운영 적용 전 불일치 검토 필요
	VerdictLevel string `json:"verdict_level"`
}

// DocGenuinenessComparePattern — 불일치 패턴 1종(A 라벨→B 라벨)과 빈도.
type DocGenuinenessComparePattern struct {
	AGenuineness string `json:"a_genuineness"`
	BGenuineness string `json:"b_genuineness"`
	Count        int    `json:"count"`
}

// DocGenuinenessOverrideEval — 사람 보정(정답) 문서 기준 모델별 정확도.
// 비교 대상(양쪽 모두 존재 + 정답 있음) 문서에서 각 모델 라벨이 정답과 일치한
// 비율. Leader는 "a"/"b"/"tie".
type DocGenuinenessOverrideEval struct {
	SampleCount int     `json:"sample_count"`
	ACorrect    int     `json:"a_correct"`
	BCorrect    int     `json:"b_correct"`
	AAccuracy   float64 `json:"a_accuracy"`
	BAccuracy   float64 `json:"b_accuracy"`
	Leader      string  `json:"leader"`
}

type BuildJobProgress struct {
	Percent        float64    `json:"percent"`
	ProcessedRows  int        `json:"processed_rows"`
	TotalRows      int        `json:"total_rows"`
	ElapsedSeconds float64    `json:"elapsed_seconds,omitempty"`
	ETASeconds     *float64   `json:"eta_seconds,omitempty"`
	Message        string     `json:"message,omitempty"`
	UpdatedAt      *time.Time `json:"updated_at,omitempty"`
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

type RuntimeStatusTemporal struct {
	Address         string `json:"address,omitempty"`
	Namespace       string `json:"namespace,omitempty"`
	TaskQueue       string `json:"task_queue,omitempty"`
	BuildTaskQueue  string `json:"build_task_queue,omitempty"`
	PersistenceMode string `json:"persistence_mode,omitempty"`
	RetentionMode   string `json:"retention_mode,omitempty"`
	RecoveryMode    string `json:"recovery_mode,omitempty"`
}

type RuntimeStatusResponse struct {
	Status         string                 `json:"status"`
	Stack          string                 `json:"stack"`
	WorkflowEngine string                 `json:"workflow_engine,omitempty"`
	StoreBackend   string                 `json:"store_backend,omitempty"`
	PlannerBackend string                 `json:"planner_backend,omitempty"`
	Temporal       *RuntimeStatusTemporal `json:"temporal,omitempty"`
}
