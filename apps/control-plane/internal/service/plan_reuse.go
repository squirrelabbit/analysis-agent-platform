package service

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

// silverone 2026-05-26 (plan reuse POC-1) — follow-up 질의가 단순 변경
// (limit 추가 / present format 변경)이면 이전 successful run의 plan을 patch해
// planner LLM 호출 없이 executor만 재실행한다. classifier가 확신하지 못하거나
// patch / validator / executor가 실패하면 기존 planner LLM 흐름으로 fallback.
//
// 가장 보수적인 정책: 애매하면 reuse하지 말고 planner로 보낸다.
//
// POC-1 범위 (지원 action 2종):
//   - change_present_format: "표로 보여줘", "차트로 보여줘", "json으로 보여줘"
//   - add_limit:             "상위 N개만 보여줘", "top N", "N개만 보여줘"
//
// add_filter / add_sort는 POC-2에서 추가.

// ReuseDecision — classifier 결과. Reused=false면 planner LLM fallback.
type ReuseDecision struct {
	Reused         bool           `json:"reused"`
	Action         string         `json:"action,omitempty"`
	ActionParams   map[string]any `json:"action_params,omitempty"`
	SourceRunID    string         `json:"source_run_id,omitempty"`
	FallbackReason string         `json:"fallback_reason,omitempty"`
}

// reusableSourceRun — reuse 대상 run의 plan + present 정보.
type reusableSourceRun struct {
	RunID       string
	Plan        map[string]any
	PresentStep map[string]any // plan 안 마지막 present skill step
}

var (
	// "상위 N개", "top N", "N개만"
	_topNPattern = regexp.MustCompile(`(?i)(?:상위|top)\s*(\d+)\s*개?|^\s*(\d+)\s*개만`)
	// "표로 보여줘" / "차트로 보여줘" / "json으로 보여줘"
	_formatTablePattern = regexp.MustCompile(`표(?:로|만)?\s*(?:보여|만들|만들어)`)
	_formatChartPattern = regexp.MustCompile(`차트(?:로|만)?\s*(?:보여|만들|만들어)`)
	_formatJsonPattern  = regexp.MustCompile(`(?i)json(?:으로|만)?\s*(?:보여|만들|만들어)`)

	// reuse 보수적으로 reject할 시그널 (집계/원인/요약/시간조건 등).
	// 매칭되면 fallback. 가장 보수적인 false negative 정책.
	_rejectPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)왜|이유|원인`),
		regexp.MustCompile(`(?i)요약|정리해줘|결론`),
		regexp.MustCompile(`(?i)평균|합계|비율`),
		regexp.MustCompile(`(?i)대비|증감|월별|분기별|기간`),
		regexp.MustCompile(`(?i)compare|join|aggregate|group\s*by`),
	}
)

// loadReusableSourceRun — thread의 가장 최근 completed run에서 plan + present
// 정보를 추출한다. run/plan/present 어느 하나라도 없으면 reuse 불가 (nil, "" 사유).
func (t *AnalysisThreadService) loadReusableSourceRun(projectID, threadID string) (*reusableSourceRun, string) {
	run, err := t.store.GetLastSuccessfulAnalysisRun(projectID, threadID)
	if err != nil {
		return nil, "no_previous_completed_run"
	}
	if len(run.ResultJSON) == 0 {
		return nil, "previous_run_result_empty"
	}
	var resultDoc struct {
		Result map[string]any `json:"-"`
	}
	// run.result_json은 analyze 응답 raw. top-level에 plan / present가 있는
	// 구조이거나 result wrapper일 수 있다 — 두 형태 모두 시도.
	var raw map[string]any
	if err := json.Unmarshal(run.ResultJSON, &raw); err != nil {
		return nil, "previous_result_unparseable"
	}
	_ = resultDoc

	plan, _ := raw["plan"].(map[string]any)
	if plan == nil {
		// fallback path — 옛 호출자가 wrapper를 한 번 더 씌운 경우
		if inner, ok := raw["result"].(map[string]any); ok {
			plan, _ = inner["plan"].(map[string]any)
		}
	}
	if plan == nil {
		return nil, "previous_run_missing_plan"
	}
	steps, _ := plan["steps"].([]any)
	if len(steps) == 0 {
		return nil, "previous_plan_empty_steps"
	}
	var present map[string]any
	for _, step := range steps {
		stepMap, ok := step.(map[string]any)
		if !ok {
			continue
		}
		if stepMap["skill"] == "present" {
			present = stepMap
		}
	}
	if present == nil {
		return nil, "previous_plan_missing_present"
	}
	return &reusableSourceRun{
		RunID:       run.RunID,
		Plan:        plan,
		PresentStep: present,
	}, ""
}

// classifyReuseAction — rule-based classifier. 매칭되면 (action, params, true).
// 매칭 안 되면 ("", nil, false). 보수적 정책: 매칭 우선순위는 reject > limit > format.
func classifyReuseAction(question string) (string, map[string]any, bool) {
	q := strings.TrimSpace(question)
	if q == "" {
		return "", nil, false
	}
	// reject signal 먼저
	for _, pattern := range _rejectPatterns {
		if pattern.MatchString(q) {
			return "", nil, false
		}
	}
	// add_limit
	if match := _topNPattern.FindStringSubmatch(q); match != nil {
		// match[1] 또는 match[2]가 N
		nStr := match[1]
		if nStr == "" {
			nStr = match[2]
		}
		n, err := strconv.Atoi(nStr)
		if err != nil || n <= 0 {
			return "", nil, false
		}
		return "add_limit", map[string]any{"n": n}, true
	}
	// change_present_format
	switch {
	case _formatTablePattern.MatchString(q):
		return "change_present_format", map[string]any{"format": "table"}, true
	case _formatChartPattern.MatchString(q):
		return "change_present_format", map[string]any{"format": "chart"}, true
	case _formatJsonPattern.MatchString(q):
		return "change_present_format", map[string]any{"format": "json"}, true
	}
	return "", nil, false
}

// patchPlanForReuse — 이전 plan에 action을 적용한 새 plan을 만든다. 원본 plan은
// 깊은 복사로 보존. 실패 시 err.
func patchPlanForReuse(sourcePlan map[string]any, action string, params map[string]any) (map[string]any, error) {
	cloned, err := deepCloneJSON(sourcePlan)
	if err != nil {
		return nil, fmt.Errorf("clone source plan: %w", err)
	}
	clonedMap, ok := cloned.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("source plan must be an object")
	}

	switch action {
	case "change_present_format":
		format, _ := params["format"].(string)
		format = strings.TrimSpace(format)
		if format == "" {
			return nil, fmt.Errorf("change_present_format requires non-empty format param")
		}
		if err := mutatePresentStep(clonedMap, func(present map[string]any) error {
			p, ok := present["params"].(map[string]any)
			if !ok {
				p = map[string]any{}
				present["params"] = p
			}
			p["format"] = format
			return nil
		}); err != nil {
			return nil, err
		}
	case "add_limit":
		n, ok := params["n"].(int)
		if !ok || n <= 0 {
			return nil, fmt.Errorf("add_limit requires positive int n")
		}
		// POC-1 가장 단순한 정책: present step의 직전 input step이 sort skill이면
		// 그 sort.params.limit을 덮어쓴다. 아니면 reuse 불가 (fallback).
		if err := mutateLimitOnSortBeforePresent(clonedMap, n); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported reuse action: %s", action)
	}

	return clonedMap, nil
}

func mutatePresentStep(plan map[string]any, fn func(map[string]any) error) error {
	steps, _ := plan["steps"].([]any)
	for _, step := range steps {
		stepMap, ok := step.(map[string]any)
		if !ok {
			continue
		}
		if stepMap["skill"] == "present" {
			return fn(stepMap)
		}
	}
	return fmt.Errorf("plan has no present step")
}

func mutateLimitOnSortBeforePresent(plan map[string]any, n int) error {
	steps, _ := plan["steps"].([]any)
	if len(steps) == 0 {
		return fmt.Errorf("plan has no steps")
	}
	var present, sortStep map[string]any
	stepByID := map[string]map[string]any{}
	for _, step := range steps {
		stepMap, ok := step.(map[string]any)
		if !ok {
			continue
		}
		id, _ := stepMap["id"].(string)
		if id != "" {
			stepByID[id] = stepMap
		}
		if stepMap["skill"] == "present" {
			present = stepMap
		}
	}
	if present == nil {
		return fmt.Errorf("plan has no present step")
	}
	presentParams, _ := present["params"].(map[string]any)
	if presentParams == nil {
		return fmt.Errorf("present step has no params")
	}
	inputID, _ := presentParams["input"].(string)
	upstream, ok := stepByID[inputID]
	if !ok {
		return fmt.Errorf("present.input %q not found in plan steps", inputID)
	}
	if upstream["skill"] != "sort" {
		// POC-1는 sort 위에 limit만 덮어쓰는 단순 케이스만. 그 외는 reuse 거부.
		return fmt.Errorf("add_limit POC-1 requires present.input to be a sort step (got skill=%v)", upstream["skill"])
	}
	sortStep = upstream
	sortParams, ok := sortStep["params"].(map[string]any)
	if !ok {
		sortParams = map[string]any{}
		sortStep["params"] = sortParams
	}
	sortParams["limit"] = n
	return nil
}

func deepCloneJSON(value any) (any, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var cloned any
	if err := json.Unmarshal(raw, &cloned); err != nil {
		return nil, err
	}
	return cloned, nil
}

// injectReuseMetadata — analyze 응답 raw bytes에 ``reuse`` 키를 merge한다.
// classifier no_match / patch fail / fallback 등 모든 분기에서 일관된 metadata
// 노출.
func injectReuseMetadata(resultRaw json.RawMessage, decision ReuseDecision) (json.RawMessage, error) {
	if len(resultRaw) == 0 {
		return resultRaw, nil
	}
	var doc map[string]any
	if err := json.Unmarshal(resultRaw, &doc); err != nil {
		return resultRaw, fmt.Errorf("inject_reuse_metadata: %w", err)
	}
	if doc == nil {
		doc = map[string]any{}
	}
	doc["reuse"] = map[string]any{
		"applied":         decision.Reused,
		"action":          decision.Action,
		"action_params":   decision.ActionParams,
		"source_run_id":   decision.SourceRunID,
		"fallback_reason": decision.FallbackReason,
	}
	return json.Marshal(doc)
}

// silverone — domain.AnalysisRun import 한 줄 안 쓰는 경고 방지용 사용처
// (Go linter false positive 회피). 실제 호출은 analysis_threads.go에서.
var _ = domain.AnalysisRun{}
