package skills

import (
	"context"
	"sort"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

type StepHook interface {
	BeforeStep(ctx context.Context, execution domain.ExecutionSummary, step domain.SkillPlanStep) (StepHookRecord, error)
	AfterStep(ctx context.Context, execution domain.ExecutionSummary, step domain.SkillPlanStep, outcome StepHookOutcome) (StepHookRecord, error)
}

type StepHookOutcome struct {
	Status         string         `json:"status"`
	ArtifactBytes  int            `json:"artifact_bytes,omitempty"`
	ArtifactRef    string         `json:"artifact_ref,omitempty"`
	UsageSummary   map[string]any `json:"usage_summary,omitempty"`
	StoredArtifact string         `json:"-"`
	ErrorMessage   string         `json:"error_message,omitempty"`
}

type StepHookRecord struct {
	Phase       string         `json:"phase"`
	StepID      string         `json:"step_id"`
	SkillName   string         `json:"skill_name"`
	DatasetName string         `json:"dataset_name,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
}

type RuntimeStepHook struct{}

func (RuntimeStepHook) BeforeStep(_ context.Context, _ domain.ExecutionSummary, step domain.SkillPlanStep) (StepHookRecord, error) {
	return StepHookRecord{
		Phase:       "before",
		StepID:      step.StepID,
		SkillName:   step.SkillName,
		DatasetName: step.DatasetName,
		Payload: map[string]any{
			"input_keys": sortedStepInputKeys(step.Inputs),
		},
	}, nil
}

func (RuntimeStepHook) AfterStep(_ context.Context, _ domain.ExecutionSummary, step domain.SkillPlanStep, outcome StepHookOutcome) (StepHookRecord, error) {
	payload := map[string]any{
		"status": outcomeStatus(outcome.Status),
	}
	if outcome.ArtifactBytes > 0 {
		payload["artifact_bytes"] = outcome.ArtifactBytes
	}
	if ref := strings.TrimSpace(outcome.ArtifactRef); ref != "" {
		payload["artifact_ref"] = ref
	}
	if usagePreview := compactUsagePreview(outcome.UsageSummary); len(usagePreview) > 0 {
		payload["usage"] = usagePreview
	}
	if message := strings.TrimSpace(outcome.ErrorMessage); message != "" {
		payload["error"] = message
	}
	return StepHookRecord{
		Phase:       "after",
		StepID:      step.StepID,
		SkillName:   step.SkillName,
		DatasetName: step.DatasetName,
		Payload:     payload,
	}, nil
}

type ExecutionProgressHook struct {
	Repo store.Repository
	Now  func() time.Time
}

func (h ExecutionProgressHook) BeforeStep(_ context.Context, execution domain.ExecutionSummary, step domain.SkillPlanStep) (StepHookRecord, error) {
	record := StepHookRecord{
		Phase:       "before",
		StepID:      step.StepID,
		SkillName:   step.SkillName,
		DatasetName: step.DatasetName,
		Payload: map[string]any{
			"status": "running",
		},
	}
	if h.Repo == nil {
		return record, nil
	}
	current, err := h.Repo.GetExecution(execution.ProjectID, execution.ExecutionID)
	if err != nil {
		return StepHookRecord{}, err
	}
	current.Events = append(current.Events, domain.ExecutionEvent{
		ExecutionID: execution.ExecutionID,
		TS:          hookNow(h.Now),
		Level:       "info",
		EventType:   "STEP_STARTED",
		Message:     "step started",
		Payload: map[string]any{
			"step_id":    step.StepID,
			"skill_name": step.SkillName,
		},
	})
	if err := h.Repo.SaveExecution(current); err != nil {
		return StepHookRecord{}, err
	}
	return record, nil
}

func (h ExecutionProgressHook) AfterStep(_ context.Context, execution domain.ExecutionSummary, step domain.SkillPlanStep, outcome StepHookOutcome) (StepHookRecord, error) {
	record := StepHookRecord{
		Phase:       "after",
		StepID:      step.StepID,
		SkillName:   step.SkillName,
		DatasetName: step.DatasetName,
		Payload: map[string]any{
			"status": outcomeStatus(outcome.Status),
		},
	}
	if h.Repo == nil {
		return record, nil
	}
	current, err := h.Repo.GetExecution(execution.ProjectID, execution.ExecutionID)
	if err != nil {
		return StepHookRecord{}, err
	}
	if current.Artifacts == nil {
		current.Artifacts = map[string]string{}
	}
	payload := map[string]any{
		"step_id":    step.StepID,
		"skill_name": step.SkillName,
		"status":     outcomeStatus(outcome.Status),
	}
	if outcome.ArtifactBytes > 0 {
		payload["artifact_bytes"] = outcome.ArtifactBytes
	}
	if ref := strings.TrimSpace(outcome.ArtifactRef); ref != "" {
		payload["artifact_ref"] = ref
	}
	if usagePreview := compactUsagePreview(outcome.UsageSummary); len(usagePreview) > 0 {
		payload["usage"] = usagePreview
	}
	if message := strings.TrimSpace(outcome.ErrorMessage); message != "" {
		payload["error"] = message
	}
	if outcomeStatus(outcome.Status) == "completed" && strings.TrimSpace(outcome.StoredArtifact) != "" {
		key := artifactKey(step)
		current.Artifacts[key] = outcome.StoredArtifact
		payload["artifact_key"] = key
	}
	eventType := "STEP_COMPLETED"
	message := "step completed"
	if outcomeStatus(outcome.Status) == "failed" {
		eventType = "STEP_FAILED"
		message = "step failed"
	}
	current.Events = append(current.Events, domain.ExecutionEvent{
		ExecutionID: execution.ExecutionID,
		TS:          hookNow(h.Now),
		Level:       "info",
		EventType:   eventType,
		Message:     message,
		Payload:     payload,
	})
	if err := h.Repo.SaveExecution(current); err != nil {
		return StepHookRecord{}, err
	}
	return record, nil
}

func activeStepHooks(hooks []StepHook) []StepHook {
	if len(hooks) == 0 {
		return []StepHook{RuntimeStepHook{}}
	}
	return hooks
}

func executeBeforeStepHooks(
	ctx context.Context,
	hooks []StepHook,
	execution domain.ExecutionSummary,
	step domain.SkillPlanStep,
) ([]StepHookRecord, error) {
	hookList := activeStepHooks(hooks)
	records := make([]StepHookRecord, 0, len(hookList))
	for _, hook := range hookList {
		record, err := hook.BeforeStep(ctx, execution, step)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func executeAfterStepHooks(
	ctx context.Context,
	hooks []StepHook,
	execution domain.ExecutionSummary,
	step domain.SkillPlanStep,
	outcome StepHookOutcome,
) ([]StepHookRecord, error) {
	hookList := activeStepHooks(hooks)
	records := make([]StepHookRecord, 0, len(hookList))
	for _, hook := range hookList {
		record, err := hook.AfterStep(ctx, execution, step, outcome)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func appendFailedStepHooks(
	ctx context.Context,
	existing []StepHookRecord,
	hooks []StepHook,
	execution domain.ExecutionSummary,
	step domain.SkillPlanStep,
	err error,
) []StepHookRecord {
	if err == nil {
		return existing
	}
	records, hookErr := executeAfterStepHooks(
		ctx,
		hooks,
		execution,
		step,
		StepHookOutcome{
			Status:       "failed",
			ErrorMessage: err.Error(),
		},
	)
	if hookErr != nil {
		return existing
	}
	return append(existing, records...)
}

func sortedStepInputKeys(inputs map[string]any) []string {
	if len(inputs) == 0 {
		return []string{}
	}
	keys := make([]string, 0, len(inputs))
	for key := range inputs {
		normalized := strings.TrimSpace(key)
		if normalized == "" {
			continue
		}
		keys = append(keys, normalized)
	}
	sort.Strings(keys)
	return keys
}

func compactUsagePreview(usage map[string]any) map[string]any {
	if len(usage) == 0 {
		return nil
	}
	preview := map[string]any{}
	for _, key := range []string{
		"provider",
		"model",
		"operation",
		"request_count",
		"total_tokens",
		"prompt_tokens",
		"vector_count",
		"estimated_cost_usd",
		"cost_estimation_status",
	} {
		if value, ok := usage[key]; ok && value != nil {
			preview[key] = value
		}
	}
	if len(preview) == 0 {
		return nil
	}
	return preview
}

func outcomeStatus(status string) string {
	normalized := strings.TrimSpace(status)
	if normalized == "" {
		return "completed"
	}
	return normalized
}

func hookNow(nowFn func() time.Time) time.Time {
	if nowFn != nil {
		return nowFn().UTC()
	}
	return time.Now().UTC()
}
