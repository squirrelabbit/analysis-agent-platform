package skills

import (
	"context"
	"sort"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

type StepHook interface {
	BeforeStep(ctx context.Context, execution domain.ExecutionSummary, step domain.SkillPlanStep) (StepHookRecord, error)
	AfterStep(ctx context.Context, execution domain.ExecutionSummary, step domain.SkillPlanStep, outcome StepHookOutcome) (StepHookRecord, error)
}

type StepHookOutcome struct {
	Status        string         `json:"status"`
	ArtifactBytes int            `json:"artifact_bytes,omitempty"`
	ArtifactRef   string         `json:"artifact_ref,omitempty"`
	UsageSummary  map[string]any `json:"usage_summary,omitempty"`
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
	return StepHookRecord{
		Phase:       "after",
		StepID:      step.StepID,
		SkillName:   step.SkillName,
		DatasetName: step.DatasetName,
		Payload:     payload,
	}, nil
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
