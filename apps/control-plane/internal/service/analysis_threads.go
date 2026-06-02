package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"
)

const (
	maxConversationContextTurns = 3
	maxConversationContextBytes = 2000

	// silverone 2026-05-26 — failed run assistant placeholder 본문. 화면에서
	// user 발언만 남는 상황을 피하고, run_id로 연결된 assistant 자리표시 메시지를
	// 남긴다. error 상세는 run.error_message에 보관. context_summary가 비어있어
	// 다음 turn의 conversation_context에서 자동 제외된다.
	failedRunAssistantPlaceholder = "분석 실행 중 오류가 발생했습니다. 조건을 조금 단순화해 다시 시도해 주세요."
)

// CreateAnalysisThread — analysis_thread 생성. 핵심 invariant:
// **thread.dataset_version_id는 생성 시점의 active dataset version으로 고정**.
// 이후 같은 thread의 messages/runs는 active version을 다시 resolve하지 않고
// 잠긴 dataset_version_id를 그대로 쓴다. dataset의 active version이 thread
// 도중에 바뀌어도 무관. 새 version으로 분석하려면 새 thread를 만든다.
//
// 자세한 모델은 vault `analysis_api_model_2026-05-26` §2 (thread version lock).
func (s *DatasetService) CreateAnalysisThread(projectID, datasetID string, input domain.AnalysisThreadCreateRequest) (domain.AnalysisThread, error) {
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.AnalysisThread{}, err
	}
	versionID := ""
	if dataset.ActiveDatasetVersionID != nil {
		versionID = strings.TrimSpace(*dataset.ActiveDatasetVersionID)
	}
	if versionID == "" {
		return domain.AnalysisThread{}, ErrInvalidArgument{Message: "dataset has no active version — upload and activate a dataset version before starting an analysis thread"}
	}
	if _, err := s.GetDatasetVersion(projectID, datasetID, versionID); err != nil {
		return domain.AnalysisThread{}, err
	}
	now := time.Now().UTC()
	thread := domain.AnalysisThread{
		ThreadID:         id.New(),
		ProjectID:        projectID,
		DatasetID:        datasetID,
		DatasetVersionID: versionID,
		Title:            analysisTitle(input.Title),
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := s.store.SaveAnalysisThread(thread); err != nil {
		return domain.AnalysisThread{}, err
	}
	return s.store.GetAnalysisThread(projectID, datasetID, thread.ThreadID)
}

func (s *DatasetService) ListAnalysisThreads(projectID, datasetID string) (domain.AnalysisThreadListResponse, error) {
	if _, err := s.GetDataset(projectID, datasetID); err != nil {
		return domain.AnalysisThreadListResponse{}, err
	}
	items, err := s.store.ListAnalysisThreads(projectID, datasetID)
	if err != nil {
		return domain.AnalysisThreadListResponse{}, err
	}
	return domain.AnalysisThreadListResponse{Items: items}, nil
}

func (s *DatasetService) GetAnalysisThread(projectID, datasetID, threadID string) (domain.AnalysisThreadDetail, error) {
	thread, err := s.store.GetAnalysisThread(projectID, datasetID, threadID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.AnalysisThreadDetail{}, ErrNotFound{Resource: "analysis thread"}
		}
		return domain.AnalysisThreadDetail{}, err
	}
	messages, err := s.store.ListAnalysisMessages(projectID, threadID)
	if err != nil {
		return domain.AnalysisThreadDetail{}, err
	}
	// silverone 2026-06-01 — assistant message에 lightweight display + plan
	// projection attach (frontend history rendering). DB 변경 X —
	// run.result_json에서 composer.display / plan keep-set만 추출해
	// message.Display / message.Plan으로 채운다. run_id가 없거나 run lookup
	// 실패 / result_json 없음 / 해당 키 없음이면 nil 유지.
	for i := range messages {
		msg := &messages[i]
		if msg.Role != "assistant" || msg.RunID == nil || strings.TrimSpace(*msg.RunID) == "" {
			continue
		}
		run, runErr := s.store.GetAnalysisRun(projectID, *msg.RunID)
		if runErr != nil {
			// run 조회 실패는 display/plan 없이 message만 반환 (best-effort).
			continue
		}
		if display := extractDisplayFromResultJSON(run.ResultJSON); display != nil {
			msg.Display = display
		}
		if plan := extractPlanFromResultJSON(run.ResultJSON); plan != nil {
			msg.Plan = plan
		}
	}
	return domain.AnalysisThreadDetail{AnalysisThread: thread, Messages: messages}, nil
}

// DeleteAnalysisThread — thread 단건 삭제 (silverone 2026-06-01, 테스트 정리용).
// project_id+dataset_id+thread_id가 모두 일치하는 thread만 삭제하며, 일치 row가
// 없거나 dataset이 다르면 404(ErrNotFound). messages/runs/rejection_events는
// FK ON DELETE CASCADE로 함께 삭제된다.
func (s *DatasetService) DeleteAnalysisThread(projectID, datasetID, threadID string) error {
	if err := s.store.DeleteAnalysisThread(projectID, datasetID, threadID); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "analysis thread"}
		}
		return err
	}
	return nil
}

func (s *DatasetService) GetAnalysisRun(projectID, datasetID, runID string) (domain.AnalysisRun, error) {
	run, err := s.store.GetAnalysisRun(projectID, runID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.AnalysisRun{}, ErrNotFound{Resource: "analysis run"}
		}
		return domain.AnalysisRun{}, err
	}
	if run.DatasetID != datasetID {
		return domain.AnalysisRun{}, ErrNotFound{Resource: "analysis run"}
	}
	return run, nil
}

// AnalyzeDatasetAsNewThread — `POST /datasets/{did}/analyze` shortcut의 service
// 진입점. (create thread + first user message + start run) 한 흐름에서 처리.
//
//	1. CreateAnalysisThread — active version 1회 resolve → thread.dataset_version_id
//	   고정 (§2).
//	2. PostAnalysisThreadMessage — user message save + run save + sync 실행.
//
// 두 번째 turn부터는 화면이 /analysis_threads/{tid}/messages로 직접 보낸다.
// 자세한 흐름은 vault `analysis_api_model_2026-05-26` §3.1.
func (s *DatasetService) AnalyzeDatasetAsNewThread(ctx context.Context, projectID, datasetID string, req AnalyzeRequest) (domain.AnalysisThreadMessageResponse, error) {
	question := strings.TrimSpace(req.UserQuestion)
	if question == "" {
		return domain.AnalysisThreadMessageResponse{}, ErrInvalidArgument{Message: "user_question is required"}
	}
	thread, err := s.CreateAnalysisThread(projectID, datasetID, domain.AnalysisThreadCreateRequest{
		Title: question,
	})
	if err != nil {
		return domain.AnalysisThreadMessageResponse{}, err
	}
	return s.PostAnalysisThreadMessage(ctx, projectID, datasetID, thread.ThreadID, domain.AnalysisThreadMessageRequest{
		Content: question,
	})
}

// PostAnalysisThreadMessage — 이어질문 진입점. 외부 endpoint는
// `POST /analysis_threads/{tid}/messages`. 첫 turn(AnalyzeDatasetAsNewThread)도
// thread 생성 직후 이 함수를 부른다.
//
// 흐름:
//  1. thread.dataset_version_id 사용 (active version 재resolve 없음, §2 잠금).
//  2. user message save + analysis_run save (status=running).
//  3. plan reuse 시도 (§5). classifier match + 이전 successful run의 plan을
//     patch해서 patched plan으로 ExecuteAnalyze. 모든 분기에서 실패하면 planner
//     LLM 흐름으로 fallback. 결과에 result.reuse metadata inject (성공/실패 모두).
//  4. 성공: run.status=completed, assistant message + context_summary save.
//  5. 실패(§4): run.status=failed + error_message + assistant placeholder message
//     save (context_summary 생략 → 다음 turn의 conversation_context에 자동 제외).
//     HTTP error는 그대로 반환해서 caller가 실패를 즉시 인지하도록 유지.
//
// conversation_context는 §6의 buildConversationContext가 assistant context_summary
// 최근 3턴/2000 bytes를 모아 worker payload에 inject한다.
func (s *DatasetService) PostAnalysisThreadMessage(
	ctx context.Context,
	projectID, datasetID, threadID string,
	input domain.AnalysisThreadMessageRequest,
) (domain.AnalysisThreadMessageResponse, error) {
	question := strings.TrimSpace(input.Content)
	if question == "" {
		return domain.AnalysisThreadMessageResponse{}, ErrInvalidArgument{Message: "message content is required"}
	}
	thread, err := s.store.GetAnalysisThread(projectID, datasetID, threadID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.AnalysisThreadMessageResponse{}, ErrNotFound{Resource: "analysis thread"}
		}
		return domain.AnalysisThreadMessageResponse{}, err
	}
	contextItems, err := s.buildConversationContext(projectID, threadID)
	if err != nil {
		return domain.AnalysisThreadMessageResponse{}, err
	}

	now := time.Now().UTC()
	userMessage := domain.AnalysisMessage{
		MessageID: id.New(),
		ThreadID:  thread.ThreadID,
		ProjectID: projectID,
		DatasetID: datasetID,
		Role:      "user",
		Content:   question,
		CreatedAt: now,
	}
	if err := s.store.SaveAnalysisMessage(userMessage); err != nil {
		return domain.AnalysisThreadMessageResponse{}, err
	}

	requestJSON := map[string]any{"user_question": question}
	if len(contextItems) > 0 {
		requestJSON["conversation_context"] = contextItems
	}
	run := domain.AnalysisRun{
		RunID:            id.New(),
		ThreadID:         thread.ThreadID,
		ProjectID:        projectID,
		DatasetID:        datasetID,
		DatasetVersionID: thread.DatasetVersionID,
		UserMessageID:    userMessage.MessageID,
		RequestJSON:      requestJSON,
		Status:           "running",
		CreatedAt:        now,
	}
	if err := s.store.SaveAnalysisRun(run); err != nil {
		return domain.AnalysisThreadMessageResponse{}, err
	}

	// silverone 2026-05-26 (plan reuse POC-1) — 단순 follow-up이면 이전
	// successful run의 plan을 patch해 planner LLM 호출 없이 executor만 실행.
	// 모든 분기 (classifier no_match / patch fail / validator fail / executor
	// fail)에서 기존 planner LLM 흐름으로 fallback. fallback 시 reuse.applied=false +
	// fallback_reason 로깅.
	reuseDecision, reusedResp, reuseOK := s.tryReusePlan(ctx, projectID, datasetID, thread, question, contextItems)
	var analysisResp AnalyzeResponse
	if reuseOK {
		analysisResp = reusedResp
	} else {
		analysisResp, err = s.ExecuteAnalyze(ctx, projectID, datasetID, thread.DatasetVersionID, AnalyzeRequest{
			UserQuestion:        question,
			ConversationContext: contextItems,
		})
	}
	completedAt := time.Now().UTC()
	if err != nil {
		message := err.Error()
		run.Status = "failed"
		run.ErrorMessage = &message
		run.CompletedAt = &completedAt
		_ = s.store.SaveAnalysisRun(run)

		// silverone 2026-05-26 — failed run UX. user 발언만 남고 assistant가 비어있는
		// 상황을 막기 위해 placeholder를 thread에 저장. caller(화면)는 HTTP error로
		// 실패를 즉시 알리고, thread reload 시에는 user/assistant 한 쌍이 보인다.
		// context_summary는 생략해 다음 turn에 conversation_context로 끌고 가지 않는다.
		runID := run.RunID
		placeholder := domain.AnalysisMessage{
			MessageID: id.New(),
			ThreadID:  thread.ThreadID,
			ProjectID: projectID,
			DatasetID: datasetID,
			Role:      "assistant",
			Content:   failedRunAssistantPlaceholder,
			RunID:     &runID,
			CreatedAt: completedAt,
		}
		_ = s.store.SaveAnalysisMessage(placeholder)

		return domain.AnalysisThreadMessageResponse{}, err
	}

	// reuse metadata는 reuse 분기든 fallback이든 항상 응답/저장 body에 inject.
	mergedResult, mergeErr := injectReuseMetadata(analysisResp.Result, reuseDecision)
	if mergeErr == nil {
		analysisResp.Result = mergedResult
	}

	run.Status = "completed"
	run.ResultJSON = append([]byte(nil), analysisResp.Result...)
	run.CompletedAt = &completedAt
	if err := s.store.SaveAnalysisRun(run); err != nil {
		return domain.AnalysisThreadMessageResponse{}, err
	}

	runID := run.RunID
	assistantMessage := domain.AnalysisMessage{
		MessageID:      id.New(),
		ThreadID:       thread.ThreadID,
		ProjectID:      projectID,
		DatasetID:      datasetID,
		Role:           "assistant",
		Content:        assistantContentFromAnalyzeResult(analysisResp.Result),
		ContextSummary: contextSummaryFromAnalyzeResult(question, analysisResp.Result),
		RunID:          &runID,
		CreatedAt:      completedAt,
	}
	if err := s.store.SaveAnalysisMessage(assistantMessage); err != nil {
		return domain.AnalysisThreadMessageResponse{}, err
	}

	// silverone 2026-06-01 (PR2) — planner가 answerable=false로 거절했고 reason이
	// unsupported_skill / missing_data_or_artifact면 rejection event 적재 (skill
	// upgrade backlog). out_of_dataset_scope는 제외. best-effort — 적재 실패가
	// 사용자 응답을 깨지 않는다 (assistant message는 이미 저장됨).
	if event, ok := rejectionEventFromResult(
		projectID, datasetID, thread.ThreadID, assistantMessage.MessageID, question, analysisResp.Result,
	); ok {
		_ = s.store.SaveRejectionEvent(event)
	}

	// silverone 2026-05-28 — frontend-safe projection.
	// DB에는 full result_json / assistant_message.context_summary / run.request_json이
	// 그대로 보존된다. 응답에서만 운영자/debug 필드를 stripping.
	return domain.AnalysisThreadMessageResponse{
		ProjectID:        projectID,
		DatasetID:        datasetID,
		ThreadID:         thread.ThreadID,
		DatasetVersionID: thread.DatasetVersionID,
		UserMessage:      userMessage.ToView(),
		AssistantMessage: assistantMessage.ToView(),
		Run:              run.ToView(),
		Mode:             "user_question",
		Result:           projectFrontendAnalyzeResult(analysisResp.Result),
	}, nil
}

// buildConversationContext — vault `analysis_api_model_2026-05-26` §6.
// 최근 assistant context_summary를 시간 역순으로 훑어 최대 3턴 / 직렬화 누적
// 2000 bytes 한도로 수집, 시간 순으로 다시 정렬해 worker에 넘긴다.
// context_summary가 비어있는 메시지(예: failed run placeholder §4)는 자동 skip.
func (s *DatasetService) buildConversationContext(projectID, threadID string) ([]map[string]any, error) {
	messages, err := s.store.ListAnalysisMessages(projectID, threadID)
	if err != nil {
		if err == store.ErrNotFound {
			return nil, ErrNotFound{Resource: "analysis thread"}
		}
		return nil, err
	}
	collected := make([]map[string]any, 0, maxConversationContextTurns)
	totalBytes := 0
	for index := len(messages) - 1; index >= 0 && len(collected) < maxConversationContextTurns; index-- {
		message := messages[index]
		if message.Role != "assistant" || len(message.ContextSummary) == 0 {
			continue
		}
		item := compactConversationContextItem(message.ContextSummary)
		if len(item) == 0 {
			continue
		}
		raw, _ := json.Marshal(item)
		if len(raw) == 0 {
			continue
		}
		if totalBytes+len(raw) > maxConversationContextBytes {
			continue
		}
		totalBytes += len(raw)
		collected = append(collected, item)
	}
	for left, right := 0, len(collected)-1; left < right; left, right = left+1, right-1 {
		collected[left], collected[right] = collected[right], collected[left]
	}
	return collected, nil
}

func compactConversationContextItem(summary map[string]any) map[string]any {
	keys := []string{"question", "answer_summary", "present_title", "row_count", "columns", "key_filters", "key_dimensions", "pending_clarification"}
	item := make(map[string]any, len(keys))
	for _, key := range keys {
		value, ok := summary[key]
		if !ok || value == nil {
			continue
		}
		if text, ok := value.(string); ok {
			text = strings.TrimSpace(text)
			if text == "" {
				continue
			}
			item[key] = truncateRunes(text, 500)
			continue
		}
		item[key] = value
	}
	return item
}

func contextSummaryFromAnalyzeResult(question string, raw json.RawMessage) map[string]any {
	// silverone 2026-05-26 (ADR-020 PR-A) — Python composer가 result.composer.
	// context_summary를 생성하면 그대로 사용. 옛 worker 응답이면 기존 deterministic
	// helper로 fallback (compat).
	if composerSummary := composerContextSummary(raw); composerSummary != nil {
		composerSummary["question"] = truncateRunes(strings.TrimSpace(question), 500)
		return composerSummary
	}
	summary := map[string]any{
		"question":       truncateRunes(strings.TrimSpace(question), 500),
		"answer_summary": assistantContentFromAnalyzeResult(raw),
	}
	root := map[string]any{}
	if err := json.Unmarshal(raw, &root); err != nil {
		return summary
	}
	present, _ := root["present"].(map[string]any)
	if len(present) == 0 {
		return summary
	}
	if title := strings.TrimSpace(fmt.Sprintf("%v", present["title"])); title != "" && title != "<nil>" {
		summary["present_title"] = truncateRunes(title, 200)
	}
	if rowCount, ok := numberAsInt(present["row_count"]); ok {
		summary["row_count"] = rowCount
	}
	if columns := presentColumns(present); len(columns) > 0 {
		summary["columns"] = columns
		summary["key_dimensions"] = columns
	}
	return summary
}

func assistantContentFromAnalyzeResult(raw json.RawMessage) string {
	// silverone 2026-05-26 (ADR-020 PR-A) — Python composer가 result.composer.
	// assistant_content를 생성하면 그대로 사용. 옛 worker 응답이면 기존 deterministic
	// helper로 fallback (compat).
	if composerContent := composerAssistantContent(raw); composerContent != "" {
		return composerContent
	}
	root := map[string]any{}
	if err := json.Unmarshal(raw, &root); err != nil {
		return "분석 결과가 생성되었습니다."
	}
	present, _ := root["present"].(map[string]any)
	if len(present) == 0 {
		return "분석 결과가 생성되었습니다."
	}
	title := strings.TrimSpace(fmt.Sprintf("%v", present["title"]))
	if title == "" || title == "<nil>" {
		title = "분석 결과"
	}
	if rowCount, ok := numberAsInt(present["row_count"]); ok {
		return fmt.Sprintf("%s (rows: %d)", title, rowCount)
	}
	return title
}

// composerAssistantContent — result.composer.assistant_content 추출. 없거나 비
// 어있으면 빈 문자열 반환 (호출자가 fallback).
func composerAssistantContent(raw json.RawMessage) string {
	root := map[string]any{}
	if err := json.Unmarshal(raw, &root); err != nil {
		return ""
	}
	composer, _ := root["composer"].(map[string]any)
	if len(composer) == 0 {
		return ""
	}
	content, _ := composer["assistant_content"].(string)
	return strings.TrimSpace(content)
}

// composerContextSummary — result.composer.context_summary 추출. composer가
// 만든 deterministic summary를 그대로 채택 (composer가 question을 안 채울 수
// 있어 호출자가 question을 덮어쓴다).
func composerContextSummary(raw json.RawMessage) map[string]any {
	root := map[string]any{}
	if err := json.Unmarshal(raw, &root); err != nil {
		return nil
	}
	composer, _ := root["composer"].(map[string]any)
	if len(composer) == 0 {
		return nil
	}
	summary, _ := composer["context_summary"].(map[string]any)
	if len(summary) == 0 {
		return nil
	}
	// shallow copy해서 호출자가 안전하게 변경 가능.
	out := make(map[string]any, len(summary))
	for k, v := range summary {
		out[k] = v
	}
	return out
}

// rejectionEventFromResult — analyze 결과(composer.metadata.reason)에서 적재 대상
// 거절 이벤트를 만든다 (silverone 2026-06-01, PR2). 적재 대상은 unsupported_skill /
// missing_data_or_artifact 두 reason뿐 — out_of_dataset_scope는 (false 반환으로)
// 저장하지 않는다. message는 composer.assistant_content(사용자 노출 거절 문구),
// capability_gap은 composer.metadata.capability_gap에서 가져온다.
func rejectionEventFromResult(
	projectID, datasetID, threadID, messageID, userQuestion string, raw json.RawMessage,
) (domain.PlannerRejectionEvent, bool) {
	if len(raw) == 0 {
		return domain.PlannerRejectionEvent{}, false
	}
	root := map[string]any{}
	if err := json.Unmarshal(raw, &root); err != nil {
		return domain.PlannerRejectionEvent{}, false
	}
	composer, _ := root["composer"].(map[string]any)
	meta, _ := composer["metadata"].(map[string]any)
	if len(meta) == 0 {
		return domain.PlannerRejectionEvent{}, false
	}
	reason := strings.TrimSpace(fmt.Sprintf("%v", meta["reason"]))
	if reason != "unsupported_skill" && reason != "missing_data_or_artifact" {
		return domain.PlannerRejectionEvent{}, false
	}
	message, _ := composer["assistant_content"].(string)
	var capabilityGap map[string]any
	if gap, ok := meta["capability_gap"].(map[string]any); ok && len(gap) > 0 {
		capabilityGap = gap
	}
	return domain.PlannerRejectionEvent{
		EventID:       id.New(),
		ProjectID:     projectID,
		DatasetID:     datasetID,
		ThreadID:      threadID,
		MessageID:     messageID,
		UserQuestion:  userQuestion,
		Reason:        reason,
		Message:       strings.TrimSpace(message),
		CapabilityGap: capabilityGap,
		CreatedAt:     time.Now().UTC(),
	}, true
}

func presentColumns(present map[string]any) []string {
	rows, _ := present["rows"].([]any)
	if len(rows) == 0 {
		return nil
	}
	first, _ := rows[0].(map[string]any)
	if len(first) == 0 {
		return nil
	}
	columns := make([]string, 0, len(first))
	for key := range first {
		columns = append(columns, key)
	}
	sort.Strings(columns)
	if len(columns) > 8 {
		columns = columns[:8]
	}
	return columns
}

func numberAsInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}

func analysisTitle(value string) string {
	return truncateRunes(strings.TrimSpace(value), 80)
}

func truncateRunes(value string, limit int) string {
	runes := []rune(strings.TrimSpace(value))
	if limit <= 0 || len(runes) <= limit {
		return string(runes)
	}
	return string(runes[:limit])
}

// tryReusePlan — plan reuse POC-1 (vault `analysis_api_model_2026-05-26` §5).
// follow-up 질의를 rule-based classifier로 분류해 reuse가 가능하면 patched plan
// 으로 ExecuteAnalyze. 어느 단계에서든 실패하면 planner LLM 흐름으로 fallback
// (보수: false negative > false positive).
//
// 반환:
//   - decision: 모든 분기에서 채워짐 (reuse.applied=false면 fallback_reason
//     포함). 호출자는 이 값을 응답 metadata로 inject.
//   - resp: reuse 성공 시 채워진 AnalyzeResponse, 실패 시 zero value.
//   - ok: reuse 흐름으로 resp를 사용해야 하는지. false면 호출자가 planner LLM
//     흐름으로 fallback.
func (s *DatasetService) tryReusePlan(
	ctx context.Context,
	projectID, datasetID string,
	thread domain.AnalysisThread,
	question string,
	contextItems []map[string]any,
) (ReuseDecision, AnalyzeResponse, bool) {
	// 1) classifier
	action, params, classified := classifyReuseAction(question)
	if !classified {
		return ReuseDecision{Reused: false, FallbackReason: "classifier_no_match"}, AnalyzeResponse{}, false
	}

	// 2) 이전 successful run 로드
	source, srcReason := s.loadReusableSourceRun(projectID, thread.ThreadID)
	if source == nil {
		return ReuseDecision{
			Reused:         false,
			Action:         action,
			ActionParams:   params,
			FallbackReason: srcReason,
		}, AnalyzeResponse{}, false
	}

	// 3) patch
	patched, err := patchPlanForReuse(source.Plan, action, params)
	if err != nil {
		return ReuseDecision{
			Reused:         false,
			Action:         action,
			ActionParams:   params,
			SourceRunID:    source.RunID,
			FallbackReason: "patch_failed: " + err.Error(),
		}, AnalyzeResponse{}, false
	}

	// 4) planner-bypass executor 호출. plan을 JSON으로 직렬화해 inject.
	planRaw, marshalErr := json.Marshal(patched)
	if marshalErr != nil {
		return ReuseDecision{
			Reused:         false,
			Action:         action,
			ActionParams:   params,
			SourceRunID:    source.RunID,
			FallbackReason: "patched_plan_marshal_failed: " + marshalErr.Error(),
		}, AnalyzeResponse{}, false
	}

	// silverone 2026-05-26 (ADR-020 PR-A) — composer가 reuse_applied 템플릿을
	// 고를 수 있게 hint를 worker로 전달.
	reuseHint := map[string]any{
		"applied":       true,
		"action":        action,
		"action_params": params,
		"source_run_id": source.RunID,
	}
	resp, err := s.ExecuteAnalyze(ctx, projectID, datasetID, thread.DatasetVersionID, AnalyzeRequest{
		Plan:          planRaw,
		ReuseMetadata: reuseHint,
	})
	if err != nil {
		return ReuseDecision{
			Reused:         false,
			Action:         action,
			ActionParams:   params,
			SourceRunID:    source.RunID,
			FallbackReason: "patched_plan_executor_failed: " + err.Error(),
		}, AnalyzeResponse{}, false
	}

	// 5) 성공
	_ = contextItems // reuse 흐름에서는 conversation_context를 inject하지 않음 (plan이 fixed)
	return ReuseDecision{
		Reused:       true,
		Action:       action,
		ActionParams: params,
		SourceRunID:  source.RunID,
	}, resp, true
}
