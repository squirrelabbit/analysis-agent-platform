package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// silverone 2026-05-21 6단계 Go 통합 — 5 case 잠금.

type analyzeFixture struct {
	t          *testing.T
	service    *DatasetService
	worker     *httptest.Server
	projectID  string
	datasetID  string
	versionID  string
	docsPath   string
	clausePath string
	genPath    string
	// captured 마지막 worker 호출의 raw payload (case 5 검증용)
	capturedPayload []byte
}

func newAnalyzeFixture(t *testing.T, workerHandler http.HandlerFunc) *analyzeFixture {
	t.Helper()
	tmpdir := t.TempDir()
	docs := filepath.Join(tmpdir, "cleaned.parquet")
	clauses := filepath.Join(tmpdir, "clause_label.jsonl")
	genuineness := filepath.Join(tmpdir, "doc_genuineness.jsonl")
	// silverone 2026-06-04 — artifact 검증(resolveAnalyzeArtifactPaths) 도입 후, fixture는
	// format-valid한 최소 내용을 써야 통과한다. parquet은 PAR1 framing, jsonl은 valid 첫 줄.
	artifactBody := map[string][]byte{
		docs:        validParquetBytes(),
		clauses:     []byte(`{"doc_id":"d1","clause":"c"}` + "\n"),
		genuineness: []byte(`{"doc_id":"d1","genuineness":"genuine_review"}` + "\n"),
	}
	for p, body := range artifactBody {
		if err := os.WriteFile(p, body, 0o644); err != nil {
			t.Fatalf("write artifact %s: %v", p, err)
		}
	}

	memory := store.NewMemoryStore()
	svc := NewDatasetService(memory, "", "", tmpdir)
	// pythonAIWorkerURL 직접 설정 (test 패턴).
	worker := httptest.NewServer(workerHandler)
	svc.pythonAIWorkerURL = worker.URL

	if err := memory.SaveProject(domain.Project{ProjectID: "p1", Name: "p"}); err != nil {
		t.Fatalf("save project: %v", err)
	}
	if err := memory.SaveDataset(domain.Dataset{ProjectID: "p1", DatasetID: "d1", Name: "d", DataType: "unstructured"}); err != nil {
		t.Fatalf("save dataset: %v", err)
	}
	version := domain.DatasetVersion{
		ProjectID:        "p1",
		DatasetID:        "d1",
		DatasetVersionID: "v1",
		StorageURI:       filepath.Join(tmpdir, "source.csv"),
		DataType:         "unstructured",
		Metadata: map[string]any{
			"clean_uri":           docs,
			"cleaned_ref":         docs,
			"clause_label_uri":    clauses,
			"clause_label_ref":    clauses,
			"doc_genuineness_uri": genuineness,
			"doc_genuineness_ref": genuineness,
		},
	}
	if err := memory.SaveDatasetVersion(version); err != nil {
		t.Fatalf("save version: %v", err)
	}

	return &analyzeFixture{
		t:          t,
		service:    svc,
		worker:     worker,
		projectID:  "p1",
		datasetID:  "d1",
		versionID:  "v1",
		docsPath:   docs,
		clausePath: clauses,
		genPath:    genuineness,
	}
}

func (f *analyzeFixture) close() {
	f.worker.Close()
}

// 1. plan direct mode 성공
func TestExecuteAnalyze_PlanDirectSuccess(t *testing.T) {
	var captured []byte
	worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// silverone 2026-06-01 (rename PR A) — Go client는 canonical /tasks/analyze
		// 만 호출한다. 옛 /tasks/analyze_v2는 worker side alias로 유지.
		if r.URL.Path != "/tasks/analyze" {
			t.Fatalf("unexpected worker path: %s (want /tasks/analyze)", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		captured = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_version":"v2","steps":[],"present":null}`))
	})
	fx := newAnalyzeFixture(t, worker)
	defer fx.close()

	resp, err := fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, fx.versionID,
		AnalyzeRequest{Plan: json.RawMessage(`{"plan_version":"v2","steps":[]}`)},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Mode != "plan" {
		t.Fatalf("mode=%q want plan", resp.Mode)
	}
	if !strings.Contains(string(resp.Result), `"plan_version":"v2"`) {
		t.Fatalf("unexpected result passthrough: %s", string(resp.Result))
	}
	// payload에 artifact_paths 정상 inject 검증 (case 5와 겹침)
	var sent map[string]any
	if err := json.Unmarshal(captured, &sent); err != nil {
		t.Fatalf("captured payload not json: %v", err)
	}
	if got, _ := sent["dataset_version_id"].(string); got != "v1" {
		t.Fatalf("dataset_version_id=%q want v1", got)
	}
	paths, _ := sent["artifact_paths"].(map[string]any)
	if paths["docs"] != fx.docsPath {
		t.Fatalf("docs path mismatch: %v want %s", paths["docs"], fx.docsPath)
	}
	if paths["clauses"] != fx.clausePath {
		t.Fatalf("clauses path mismatch: %v", paths["clauses"])
	}
	if paths["genuineness"] != fx.genPath {
		t.Fatalf("genuineness path mismatch: %v", paths["genuineness"])
	}
}

// 2. user_question mode 성공 (fake worker가 planner 응답 반환)
func TestExecuteAnalyze_UserQuestionSuccess(t *testing.T) {
	worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_version":"v2","steps":[],"planner":{"prompt_version":"planner-v2-anthropic-v1"}}`))
	})
	fx := newAnalyzeFixture(t, worker)
	defer fx.close()

	resp, err := fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, fx.versionID,
		AnalyzeRequest{UserQuestion: "작년과 올해의 aspect 증감수치 계산해줘"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Mode != "user_question" {
		t.Fatalf("mode=%q want user_question", resp.Mode)
	}
	if !strings.Contains(string(resp.Result), `"prompt_version":"planner-v2-anthropic-v1"`) {
		t.Fatalf("planner metadata not passthrough: %s", string(resp.Result))
	}
}

// 3. plan + user_question 동시 입력 → ambiguous fail
func TestExecuteAnalyze_AmbiguousRequest(t *testing.T) {
	fx := newAnalyzeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("worker should not be called for ambiguous request")
	}))
	defer fx.close()

	_, err := fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, fx.versionID,
		AnalyzeRequest{
			Plan:         json.RawMessage(`{"plan_version":"v2","steps":[]}`),
			UserQuestion: "x",
		},
	)
	if err == nil {
		t.Fatal("expected ambiguous error")
	}
	if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("expected ErrInvalidArgument, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "exactly one of") {
		t.Fatalf("error message should mention 'exactly one of': %v", err)
	}
}

// 4. artifact path 누락 시 fail
func TestExecuteAnalyze_MissingArtifact(t *testing.T) {
	fx := newAnalyzeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("worker should not be called when artifact missing")
	}))
	defer fx.close()

	// version metadata에서 clauses ref를 비우고, 새 버전으로 교체
	bad := domain.DatasetVersion{
		ProjectID:        fx.projectID,
		DatasetID:        fx.datasetID,
		DatasetVersionID: "v_bad",
		StorageURI:       fx.docsPath,
		DataType:         "unstructured",
		Metadata: map[string]any{
			"clean_uri":   fx.docsPath,
			"cleaned_ref": fx.docsPath,
			// clause_label_ref + doc_genuineness_ref 누락
		},
	}
	if err := fx.service.store.SaveDatasetVersion(bad); err != nil {
		t.Fatalf("save bad version: %v", err)
	}

	_, err := fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, "v_bad",
		AnalyzeRequest{UserQuestion: "x"},
	)
	if err == nil {
		t.Fatal("expected missing artifact error")
	}
	if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("expected ErrInvalidArgument, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "clauses") || !strings.Contains(err.Error(), "genuineness") {
		t.Fatalf("error should mention missing 'clauses' + 'genuineness': %v", err)
	}
}

// 5. artifact가 metadata에는 있지만 disk에 없으면 fail
func TestExecuteAnalyze_ArtifactOnDiskMissing(t *testing.T) {
	fx := newAnalyzeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("worker should not be called when artifact missing on disk")
	}))
	defer fx.close()
	if err := os.Remove(fx.clausePath); err != nil {
		t.Fatalf("remove clause path: %v", err)
	}

	_, err := fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, fx.versionID,
		AnalyzeRequest{UserQuestion: "x"},
	)
	if err == nil {
		t.Fatal("expected missing-on-disk error")
	}
	if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("expected ErrInvalidArgument, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "missing on disk") {
		t.Fatalf("error should mention 'missing on disk': %v", err)
	}
}

// 5a. 전처리 빌드가 진행 중(status=running)이면 'missing on disk' 대신 상태 안내로 차단.
// silverone 2026-06-26 — ref(경로)는 빌드 running 시점에 미리 써지므로, 파일이 아직 없어도
// status를 먼저 보고 "전처리 진행 중"을 알려준다(전처리 미완에 채팅친 케이스).
func TestExecuteAnalyze_BuildRunningGuard(t *testing.T) {
	fx := newAnalyzeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("worker should not be called while preprocessing is running")
	}))
	defer fx.close()
	// artifact 파일은 존재하지만 clause_label 빌드가 진행 중인 상태로 갱신.
	v, err := fx.service.store.GetDatasetVersion(fx.projectID, fx.versionID)
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	v.Metadata["clause_label_status"] = "running"
	if err := fx.service.store.SaveDatasetVersion(v); err != nil {
		t.Fatalf("save version: %v", err)
	}

	_, err = fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, fx.versionID,
		AnalyzeRequest{UserQuestion: "x"},
	)
	if err == nil {
		t.Fatal("expected build-running guard error")
	}
	if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("expected ErrInvalidArgument, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "진행 중") {
		t.Fatalf("error should mention '진행 중': %v", err)
	}
	if strings.Contains(err.Error(), "missing on disk") {
		t.Fatalf("running 상태에선 기술 메시지(missing on disk)가 아니라 상태 안내여야 함: %v", err)
	}
}

// 5b. artifact가 disk에 있지만 손상(corrupt parquet)이면 worker 호출 전에 fail.
// silverone 2026-06-04 — artifact 검증 도입.
func TestExecuteAnalyze_CorruptParquetArtifact(t *testing.T) {
	fx := newAnalyzeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("worker should not be called when docs parquet is corrupt")
	}))
	defer fx.close()
	// 유효 parquet(PAR1 framing)을 PAR1 없는 garbage로 덮어써 손상 재현.
	if err := os.WriteFile(fx.docsPath, []byte("not-a-parquet-file"), 0o644); err != nil {
		t.Fatalf("corrupt docs: %v", err)
	}

	_, err := fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, fx.versionID,
		AnalyzeRequest{UserQuestion: "x"},
	)
	if err == nil {
		t.Fatal("expected corrupt-parquet error")
	}
	if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("expected ErrInvalidArgument, got %T: %v", err, err)
	}
	// 운영자가 바로 알 수 있게 label(docs)/format(parquet)/원인(PAR1)이 메시지에.
	for _, want := range []string{"docs", "parquet", "PAR1"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error should mention %q: %v", want, err)
		}
	}
}

// 5c. corrupt jsonl(clauses)도 worker 호출 전에 fail.
func TestExecuteAnalyze_CorruptJSONLArtifact(t *testing.T) {
	fx := newAnalyzeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("worker should not be called when clauses jsonl is corrupt")
	}))
	defer fx.close()
	if err := os.WriteFile(fx.clausePath, []byte("this is not json\n"), 0o644); err != nil {
		t.Fatalf("corrupt clauses: %v", err)
	}

	_, err := fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, fx.versionID,
		AnalyzeRequest{UserQuestion: "x"},
	)
	if err == nil {
		t.Fatal("expected corrupt-jsonl error")
	}
	if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("expected ErrInvalidArgument, got %T: %v", err, err)
	}
	for _, want := range []string{"clauses", "jsonl"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error should mention %q: %v", want, err)
		}
	}
}

//  7. active version 자동 resolve — dataset.active_dataset_version_id=v1로 설정 후
//     /datasets/{did}/analyze_v2 호출이 같은 결과를 돌려주는지.
func TestExecuteAnalyzeOnActiveVersion_Success(t *testing.T) {
	var captured []byte
	worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		captured = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_version":"v2","steps":[]}`))
	})
	fx := newAnalyzeFixture(t, worker)
	defer fx.close()
	// dataset에 active version 표시
	dataset := domain.Dataset{
		ProjectID:              fx.projectID,
		DatasetID:              fx.datasetID,
		Name:                   "d",
		DataType:               "unstructured",
		ActiveDatasetVersionID: ptrString(fx.versionID),
	}
	if err := fx.service.store.SaveDataset(dataset); err != nil {
		t.Fatalf("save dataset: %v", err)
	}

	resp, err := fx.service.ExecuteAnalyzeOnActiveVersion(
		context.Background(),
		fx.projectID, fx.datasetID,
		AnalyzeRequest{UserQuestion: "x"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.VersionID != fx.versionID {
		t.Fatalf("response version_id=%q want %q", resp.VersionID, fx.versionID)
	}
	if resp.Mode != "user_question" {
		t.Fatalf("mode=%q want user_question", resp.Mode)
	}
	// payload에 active version id가 inject됐는지 (ExecuteAnalyze 재사용 검증)
	var sent map[string]any
	if err := json.Unmarshal(captured, &sent); err != nil {
		t.Fatalf("captured: %v", err)
	}
	if sent["dataset_version_id"] != fx.versionID {
		t.Fatalf("payload dataset_version_id=%v want %s", sent["dataset_version_id"], fx.versionID)
	}
	paths, _ := sent["artifact_paths"].(map[string]any)
	if paths["docs"] != fx.docsPath {
		t.Fatalf("artifact path not reused: docs=%v want %s", paths["docs"], fx.docsPath)
	}
}

// 8. active version 없는 dataset → ErrInvalidArgument
func TestExecuteAnalyzeOnActiveVersion_NoActiveVersion(t *testing.T) {
	fx := newAnalyzeFixture(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("worker should not be called without active version")
	}))
	defer fx.close()
	// fixture는 dataset에 active version 미설정 — 기본 상태 그대로 사용

	_, err := fx.service.ExecuteAnalyzeOnActiveVersion(
		context.Background(),
		fx.projectID, fx.datasetID,
		AnalyzeRequest{UserQuestion: "x"},
	)
	if err == nil {
		t.Fatal("expected ErrInvalidArgument for missing active version")
	}
	if _, ok := err.(ErrInvalidArgument); !ok {
		t.Fatalf("expected ErrInvalidArgument, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "active version") {
		t.Fatalf("error should mention 'active version': %v", err)
	}
}

func TestAnalyzeDatasetAsNewThreadStoresMessagesRunAndFollowupContext(t *testing.T) {
	var capturedPayloads [][]byte
	worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		capturedPayloads = append(capturedPayloads, body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
		  "plan_version":"v2",
		  "steps":[],
		  "present":{
		    "title":"주요 이슈",
		    "row_count":2,
		    "rows":[{"aspect":"parking","n":3},{"aspect":"crowd","n":2}]
		  },
		  "planner":{"prompt_version":"planner-v2-anthropic-v1"}
		}`))
	})
	fx := newAnalyzeFixture(t, worker)
	defer fx.close()
	if err := fx.service.store.SaveDataset(domain.Dataset{
		ProjectID:              fx.projectID,
		DatasetID:              fx.datasetID,
		Name:                   "d",
		DataType:               "unstructured",
		ActiveDatasetVersionID: ptrString(fx.versionID),
	}); err != nil {
		t.Fatalf("save active dataset: %v", err)
	}

	first, err := fx.service.AnalyzeDatasetAsNewThread(
		context.Background(),
		fx.projectID,
		fx.datasetID,
		AnalyzeRequest{UserQuestion: "부정 리뷰에서 큰 이슈는?"},
	)
	if err != nil {
		t.Fatalf("AnalyzeDatasetAsNewThread: %v", err)
	}
	if first.ThreadID == "" || first.Run.RunID == "" {
		t.Fatalf("expected thread/run ids: %+v", first)
	}
	if first.Run.Status != "completed" {
		t.Fatalf("expected completed run: %+v", first.Run)
	}
	if !strings.Contains(first.AssistantMessage.Content, "주요 이슈") {
		t.Fatalf("assistant content should summarize present result: %+v", first.AssistantMessage)
	}

	detail, err := fx.service.GetAnalysisThread(fx.projectID, fx.datasetID, first.ThreadID)
	if err != nil {
		t.Fatalf("GetAnalysisThread: %v", err)
	}
	if len(detail.Messages) != 2 {
		t.Fatalf("expected user+assistant messages, got %+v", detail.Messages)
	}
	run, err := fx.service.GetAnalysisRun(fx.projectID, fx.datasetID, first.Run.RunID)
	if err != nil {
		t.Fatalf("GetAnalysisRun: %v", err)
	}
	if !strings.Contains(string(run.ResultJSON), `"present"`) {
		t.Fatalf("run should retain full result_json: %s", string(run.ResultJSON))
	}

	second, err := fx.service.PostAnalysisThreadMessage(
		context.Background(),
		fx.projectID,
		fx.datasetID,
		first.ThreadID,
		domain.AnalysisThreadMessageRequest{Content: "그중 긍정 리뷰만 보면?"},
	)
	if err != nil {
		t.Fatalf("PostAnalysisThreadMessage: %v", err)
	}
	if second.ThreadID != first.ThreadID {
		t.Fatalf("followup should reuse thread: %+v", second)
	}
	if len(capturedPayloads) != 2 {
		t.Fatalf("expected two worker calls, got %d", len(capturedPayloads))
	}
	var sent map[string]any
	if err := json.Unmarshal(capturedPayloads[1], &sent); err != nil {
		t.Fatalf("second worker payload: %v", err)
	}
	contextItems, ok := sent["conversation_context"].([]any)
	if !ok || len(contextItems) != 1 {
		t.Fatalf("expected one compact conversation_context item: %s", string(capturedPayloads[1]))
	}
	item, _ := contextItems[0].(map[string]any)
	if item["question"] != "부정 리뷰에서 큰 이슈는?" {
		t.Fatalf("context should include prior question summary, got %+v", item)
	}
	if _, ok := item["result_json"]; ok {
		t.Fatalf("context must not include full result_json: %+v", item)
	}
}

func ptrString(s string) *string { return &s }

// silverone 2026-05-26 (ADR-020 PR-A) — composer assistant_content / context_summary가
// 응답에 있으면 그대로 assistant_message에 저장된다는 잠금.
func TestAnalyzeDatasetAsNewThread_UsesComposerOutput(t *testing.T) {
	worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
		  "plan_version":"v2",
		  "steps":[],
		  "present":{
		    "title":"aspect별 카운트",
		    "row_count":3,
		    "total_rows":3,
		    "returned_rows":3,
		    "rows":[{"aspect":"food","n":2}]
		  },
		  "composer":{
		    "assistant_content":"분석 결과 3건을 table 형식으로 정리했습니다.",
		    "display":{"type":"table","title":"aspect별 카운트","rows":[{"aspect":"food","n":2}],"total_rows":3,"returned_rows":3,"truncated":false,"max_rows":1000},
		    "context_summary":{"present_title":"aspect별 카운트","total_rows":3,"row_count":3,"returned_rows":3,"columns":["aspect","n"],"key_dimensions":["aspect","n"],"answer_summary":"분석 결과 3건을 table 형식으로 정리했습니다."},
		    "metadata":{"mode":"deterministic","template":"table_normal","fallback_reason":null}
		  }
		}`))
	})
	fx := newAnalyzeFixture(t, worker)
	defer fx.close()
	if err := fx.service.store.SaveDataset(domain.Dataset{
		ProjectID:              fx.projectID,
		DatasetID:              fx.datasetID,
		Name:                   "d",
		DataType:               "unstructured",
		ActiveDatasetVersionID: ptrString(fx.versionID),
	}); err != nil {
		t.Fatalf("save active dataset: %v", err)
	}

	resp, err := fx.service.AnalyzeDatasetAsNewThread(
		context.Background(),
		fx.projectID,
		fx.datasetID,
		AnalyzeRequest{UserQuestion: "aspect별 카운트 알려줘"},
	)
	if err != nil {
		t.Fatalf("AnalyzeDatasetAsNewThread: %v", err)
	}
	if resp.AssistantMessage.Content != "분석 결과 3건을 table 형식으로 정리했습니다." {
		t.Fatalf("expected composer assistant_content, got %q", resp.AssistantMessage.Content)
	}
	// silverone 2026-05-28 — frontend-safe projection 정책 잠금. assistant_message
	// 응답 view에는 context_summary가 없고, DB의 AnalysisMessage에는 보존된다.
	messages, err := fx.service.store.ListAnalysisMessages(fx.projectID, resp.ThreadID)
	if err != nil {
		t.Fatalf("ListAnalysisMessages: %v", err)
	}
	var savedAssistant domain.AnalysisMessage
	for _, msg := range messages {
		if msg.Role == "assistant" {
			savedAssistant = msg
		}
	}
	if savedAssistant.ContextSummary["present_title"] != "aspect별 카운트" {
		t.Fatalf("DB assistant message should preserve context_summary.present_title, got %+v", savedAssistant.ContextSummary)
	}
	if savedAssistant.ContextSummary["question"] != "aspect별 카운트 알려줘" {
		t.Fatalf("DB assistant message context_summary.question should be set by caller, got %+v", savedAssistant.ContextSummary["question"])
	}
}

// silverone 2026-05-26 (commit c4299259) — failed run UX 잠금.
// PostAnalysisThreadMessage에서 분석이 실패하면 thread에 assistant placeholder가
// 저장되고, run.status=failed + run_id가 placeholder.run_id로 연결되어야 한다.
// HTTP error는 그대로 caller에 반환 (응답 정책 §4).
func TestPostAnalysisThreadMessage_FailureSavesPlaceholderAssistantMessage(t *testing.T) {
	worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"planner blew up"}`))
	})
	fx := newAnalyzeFixture(t, worker)
	defer fx.close()
	if err := fx.service.store.SaveDataset(domain.Dataset{
		ProjectID:              fx.projectID,
		DatasetID:              fx.datasetID,
		Name:                   "d",
		DataType:               "unstructured",
		ActiveDatasetVersionID: ptrString(fx.versionID),
	}); err != nil {
		t.Fatalf("save active dataset: %v", err)
	}

	_, err := fx.service.AnalyzeDatasetAsNewThread(
		context.Background(),
		fx.projectID,
		fx.datasetID,
		AnalyzeRequest{UserQuestion: "왜 실패?"},
	)
	if err == nil {
		t.Fatalf("expected HTTP error from worker, got nil")
	}

	threads, err := fx.service.ListAnalysisThreads(fx.projectID, fx.datasetID)
	if err != nil {
		t.Fatalf("ListAnalysisThreads: %v", err)
	}
	if len(threads.Items) != 1 {
		t.Fatalf("expected 1 thread, got %d", len(threads.Items))
	}
	threadID := threads.Items[0].ThreadID

	detail, err := fx.service.GetAnalysisThread(fx.projectID, fx.datasetID, threadID)
	if err != nil {
		t.Fatalf("GetAnalysisThread: %v", err)
	}
	if len(detail.Messages) != 2 {
		t.Fatalf("expected user+assistant placeholder, got %d messages: %+v", len(detail.Messages), detail.Messages)
	}
	user := detail.Messages[0]
	assistant := detail.Messages[1]
	if user.Role != "user" || assistant.Role != "assistant" {
		t.Fatalf("roles mismatch: user=%s assistant=%s", user.Role, assistant.Role)
	}
	if !strings.Contains(assistant.Content, "오류") {
		t.Fatalf("assistant placeholder content should mention error: %q", assistant.Content)
	}
	// context_summary가 비어 있어야 다음 turn conversation_context에서 자동 제외됨.
	if len(assistant.ContextSummary) != 0 {
		t.Fatalf("failed placeholder should not carry context_summary, got %+v", assistant.ContextSummary)
	}
	if assistant.RunID == nil || *assistant.RunID == "" {
		t.Fatalf("placeholder should link to failed run, got run_id=%v", assistant.RunID)
	}

	run, err := fx.service.GetAnalysisRun(fx.projectID, fx.datasetID, *assistant.RunID)
	if err != nil {
		t.Fatalf("GetAnalysisRun: %v", err)
	}
	if run.Status != "failed" {
		t.Fatalf("expected run.status=failed, got %s", run.Status)
	}
	if run.ErrorMessage == nil || *run.ErrorMessage == "" {
		t.Fatalf("run.error_message should be populated, got %v", run.ErrorMessage)
	}
}

// 6. payload inject 정확성 — docs_extra_columns가 서버 SourceSummary에서
// auto-derive되어 worker payload에 inject되는지 (J-안, 2026-05-22). 클라이언트는
// 컬럼 메타를 보내지 않고, 서버가 storage_uri의 컬럼 중 표준 docs 컬럼이
// 아닌 것들만 자동으로 추려 전달한다.
func TestExecuteAnalyze_DocsExtraColumnsAutoDerived(t *testing.T) {
	var captured []byte
	worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		captured = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_version":"v2","steps":[]}`))
	})
	fx := newAnalyzeFixture(t, worker)
	defer fx.close()

	// fixture는 storage_uri만 경로로 들고 실제 파일이 없다. dataset-specific
	// 컬럼이 있는 진짜 CSV를 만들어 storage_uri 자리에 두고, loadDatasetSourceSummary가
	// 그 컬럼들을 회수할 수 있게 한다.
	csvPath := filepath.Join(filepath.Dir(fx.docsPath), "source.csv")
	csvBody := "title,body,channel,region\nhello,world,blog,seoul\n"
	if err := os.WriteFile(csvPath, []byte(csvBody), 0o644); err != nil {
		t.Fatalf("write csv: %v", err)
	}

	_, err := fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, fx.versionID,
		AnalyzeRequest{UserQuestion: "x"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(string(captured), `"docs_extra_columns"`) {
		t.Fatalf("docs_extra_columns missing from worker payload: %s", string(captured))
	}
	for _, expected := range []string{`"title"`, `"body"`, `"channel"`, `"region"`} {
		if !strings.Contains(string(captured), expected) {
			t.Fatalf("expected %s in worker payload: %s", expected, string(captured))
		}
	}
}

// taxonomy-driven config Phase 3-B wire (silverone 2026-05-27) — control plane
// 이 clause_label artifact summary에서 taxonomy_id/hash를 추출해 worker
// payload에 inject하는지 잠금. worker 측은 Phase 3-B에서 받을 준비됨.

// legacy version (clause_label_summary 없음) — worker payload에 clause_label_
// metadata 필드가 없어야 한다 (worker가 legacy_missing 분기로 처리).
func TestExecuteAnalyze_TaxonomyMetadataLegacyMissing(t *testing.T) {
	var captured []byte
	worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		captured = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_version":"v2","steps":[]}`))
	})
	fx := newAnalyzeFixture(t, worker)
	defer fx.close()

	_, err := fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, fx.versionID,
		AnalyzeRequest{Plan: json.RawMessage(`{"plan_version":"v2","steps":[]}`)},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var sent map[string]any
	if err := json.Unmarshal(captured, &sent); err != nil {
		t.Fatalf("captured payload not json: %v", err)
	}
	if _, present := sent["clause_label_metadata"]; present {
		t.Fatalf("clause_label_metadata should be omitted for legacy version: %v", sent["clause_label_metadata"])
	}
}

// clause_label_summary에 taxonomy_id/hash가 모두 있는 경우 — payload에
// clause_label_metadata 필드로 inject되어야 한다.
func TestExecuteAnalyze_TaxonomyMetadataInjected(t *testing.T) {
	var captured []byte
	worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		captured = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_version":"v2","steps":[]}`))
	})
	fx := newAnalyzeFixture(t, worker)
	defer fx.close()

	// fixture version의 metadata에 clause_label_summary inject.
	version, err := fx.service.GetDatasetVersion(fx.projectID, fx.datasetID, fx.versionID)
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	version.Metadata["clause_label_summary"] = map[string]any{
		"taxonomy_id":   "festival-v2",
		"taxonomy_hash": "abc123",
		"clause_count":  10,
	}
	if err := fx.service.store.SaveDatasetVersion(version); err != nil {
		t.Fatalf("save version: %v", err)
	}

	_, err = fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, fx.versionID,
		AnalyzeRequest{Plan: json.RawMessage(`{"plan_version":"v2","steps":[]}`)},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var sent map[string]any
	if err := json.Unmarshal(captured, &sent); err != nil {
		t.Fatalf("captured payload not json: %v", err)
	}
	taxonomyMeta, ok := sent["clause_label_metadata"].(map[string]any)
	if !ok {
		t.Fatalf("clause_label_metadata missing or wrong type: %v", sent["clause_label_metadata"])
	}
	if got, _ := taxonomyMeta["taxonomy_id"].(string); got != "festival-v2" {
		t.Fatalf("taxonomy_id=%q want festival-v2", got)
	}
	if got, _ := taxonomyMeta["taxonomy_hash"].(string); got != "abc123" {
		t.Fatalf("taxonomy_hash=%q want abc123", got)
	}
}

// summary에 taxonomy_id만 있고 hash가 없는 부분 정보 케이스 — id만 inject.
func TestExecuteAnalyze_TaxonomyMetadataPartial(t *testing.T) {
	var captured []byte
	worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		captured = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_version":"v2","steps":[]}`))
	})
	fx := newAnalyzeFixture(t, worker)
	defer fx.close()

	version, err := fx.service.GetDatasetVersion(fx.projectID, fx.datasetID, fx.versionID)
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	version.Metadata["clause_label_summary"] = map[string]any{
		"taxonomy_id": "festival-v2",
		// taxonomy_hash 없음
	}
	if err := fx.service.store.SaveDatasetVersion(version); err != nil {
		t.Fatalf("save version: %v", err)
	}

	_, err = fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, fx.versionID,
		AnalyzeRequest{Plan: json.RawMessage(`{"plan_version":"v2","steps":[]}`)},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var sent map[string]any
	if err := json.Unmarshal(captured, &sent); err != nil {
		t.Fatalf("captured payload not json: %v", err)
	}
	taxonomyMeta, ok := sent["clause_label_metadata"].(map[string]any)
	if !ok {
		t.Fatalf("clause_label_metadata missing: %v", sent["clause_label_metadata"])
	}
	if _, present := taxonomyMeta["taxonomy_id"]; !present {
		t.Fatalf("taxonomy_id missing from inject: %v", taxonomyMeta)
	}
	if _, present := taxonomyMeta["taxonomy_hash"]; present {
		t.Fatalf("taxonomy_hash should be omitted when summary lacks it: %v", taxonomyMeta)
	}
}

// summary는 있지만 taxonomy_* 필드 모두 누락 — clause_label_metadata 자체 생략.
func TestExecuteAnalyze_TaxonomyMetadataSummaryWithoutTaxonomy(t *testing.T) {
	var captured []byte
	worker := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		captured = body
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"plan_version":"v2","steps":[]}`))
	})
	fx := newAnalyzeFixture(t, worker)
	defer fx.close()

	version, err := fx.service.GetDatasetVersion(fx.projectID, fx.datasetID, fx.versionID)
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	version.Metadata["clause_label_summary"] = map[string]any{
		"clause_count": 5,
		// taxonomy_* 없음 — 옛 artifact 시뮬레이션
	}
	if err := fx.service.store.SaveDatasetVersion(version); err != nil {
		t.Fatalf("save version: %v", err)
	}

	_, err = fx.service.ExecuteAnalyze(
		context.Background(),
		fx.projectID, fx.datasetID, fx.versionID,
		AnalyzeRequest{Plan: json.RawMessage(`{"plan_version":"v2","steps":[]}`)},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var sent map[string]any
	if err := json.Unmarshal(captured, &sent); err != nil {
		t.Fatalf("captured payload not json: %v", err)
	}
	if _, present := sent["clause_label_metadata"]; present {
		t.Fatalf("clause_label_metadata should be omitted when summary lacks taxonomy fields: %v", sent["clause_label_metadata"])
	}
}
