package skills

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/obs"
	"analysis-support-platform/control-plane/internal/registry"
)

type CompositeRunner struct {
	Structured   StructuredPlanRunner
	Unstructured UnstructuredPlanRunner
}

func (r CompositeRunner) Run(ctx context.Context, execution domain.ExecutionSummary) (ExecutionRunResult, error) {
	l := obs.FromContext(ctx)
	structuredExecution, structuredCount, unstructuredExecution, unstructuredCount, unsupported := splitExecution(execution)

	// Log per-skill routing decisions.
	for _, step := range structuredExecution.Plan.Steps {
		l.Info("skill routing decision",
			"event", "skill.routing.decision",
			"skill_name", step.SkillName,
			"target", "duckdb",
			"reason", "registry_engine",
		)
	}
	for _, step := range unstructuredExecution.Plan.Steps {
		l.Info("skill routing decision",
			"event", "skill.routing.decision",
			"skill_name", step.SkillName,
			"target", "python_ai",
			"reason", "registry_engine",
		)
	}
	for _, name := range unsupported {
		l.Warn("skill routing decision",
			"event", "skill.routing.decision",
			"skill_name", name,
			"target", "none",
			"reason", "not_registered",
		)
	}

	result := ExecutionRunResult{
		Artifacts: map[string]string{},
		Notes:     []string{},
	}
	engines := []string{}

	if structuredCount > 0 {
		if r.Structured == nil {
			return ExecutionRunResult{}, errors.New("structured runner is required")
		}
		inputShape := skillGroupShape(structuredExecution.Plan.Steps)
		l.Info("skill execution started",
			"event", "skill.executed.started",
			"skill_name", "duckdb",
			"runtime_layer", "duckdb",
			"input_shape", inputShape,
		)
		groupStart := time.Now()
		runResult, err := r.Structured.Run(ctx, structuredExecution)
		if err != nil {
			l.Error("skill execution failed",
				"event", "skill.executed.failed",
				"skill_name", "duckdb",
				"duration_ms", time.Since(groupStart).Milliseconds(),
				"error_category", classifySkillError(err),
			)
			return ExecutionRunResult{}, err
		}
		outputShape := fmt.Sprintf("processed_steps=%d, artifact_count=%d", runResult.ProcessedSteps, len(runResult.Artifacts))
		l.Info("skill execution completed",
			"event", "skill.executed.completed",
			"skill_name", "duckdb",
			"runtime_layer", "duckdb",
			"duration_ms", time.Since(groupStart).Milliseconds(),
			"output_shape", outputShape,
		)
		mergeRunResult(&result, runResult)
		engines = appendEngine(engines, runResult.Engine)
	}

	if unstructuredCount > 0 {
		if r.Unstructured == nil {
			return ExecutionRunResult{}, errors.New("unstructured runner is required")
		}
		inputShape := skillGroupShape(unstructuredExecution.Plan.Steps)
		l.Info("skill execution started",
			"event", "skill.executed.started",
			"skill_name", "python_ai",
			"runtime_layer", "python-ai",
			"input_shape", inputShape,
		)
		groupStart := time.Now()
		runResult, err := r.Unstructured.Run(ctx, unstructuredExecution)
		if err != nil {
			l.Error("skill execution failed",
				"event", "skill.executed.failed",
				"skill_name", "python_ai",
				"duration_ms", time.Since(groupStart).Milliseconds(),
				"error_category", classifySkillError(err),
			)
			return ExecutionRunResult{}, err
		}
		outputShape := fmt.Sprintf("processed_steps=%d, artifact_count=%d", runResult.ProcessedSteps, len(runResult.Artifacts))
		l.Info("skill execution completed",
			"event", "skill.executed.completed",
			"skill_name", "python_ai",
			"runtime_layer", "python-ai",
			"duration_ms", time.Since(groupStart).Milliseconds(),
			"output_shape", outputShape,
		)
		mergeRunResult(&result, runResult)
		engines = appendEngine(engines, runResult.Engine)
	}

	for _, skillName := range unsupported {
		result.Notes = append(result.Notes, fmt.Sprintf("unsupported skill skipped: %s", skillName))
	}

	if len(engines) == 0 {
		result.Engine = "none"
	} else {
		result.Engine = strings.Join(engines, "+")
	}

	return result, nil
}

// skillGroupShape builds a concise input_shape summary for a step group.
func skillGroupShape(steps []domain.SkillPlanStep) string {
	names := make([]string, len(steps))
	for i, s := range steps {
		names[i] = s.SkillName
	}
	return fmt.Sprintf("step_count=%d, skills=[%s]", len(steps), strings.Join(names, ","))
}

// classifySkillError returns a short error category string.
func classifySkillError(err error) string {
	if err == nil {
		return ""
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	switch {
	case strings.Contains(msg, "context deadline exceeded") || strings.Contains(msg, "context canceled"):
		return "timeout"
	case strings.Contains(msg, "not found") || strings.Contains(msg, "404"):
		return "not_found"
	case strings.Contains(msg, "invalid") || strings.Contains(msg, "bad request") || strings.Contains(msg, "400"):
		return "invalid_input"
	case strings.Contains(msg, "python ai worker returned"):
		return "worker_error"
	default:
		return "internal_error"
	}
}

func splitExecution(execution domain.ExecutionSummary) (domain.ExecutionSummary, int, domain.ExecutionSummary, int, []string) {
	structuredExecution := execution
	unstructuredExecution := execution
	structuredSteps := make([]domain.SkillPlanStep, 0, len(execution.Plan.Steps))
	unstructuredSteps := make([]domain.SkillPlanStep, 0, len(execution.Plan.Steps))
	unsupported := []string{}

	for _, step := range execution.Plan.Steps {
		definition, ok := registry.Skill(step.SkillName)
		if !ok {
			unsupported = append(unsupported, step.SkillName)
			continue
		}
		switch definition.Engine {
		case "duckdb":
			structuredSteps = append(structuredSteps, step)
		case "python-ai":
			unstructuredSteps = append(unstructuredSteps, step)
		default:
			unsupported = append(unsupported, step.SkillName)
		}
	}

	structuredExecution.Plan.Steps = structuredSteps
	unstructuredExecution.Plan.Steps = unstructuredSteps
	return structuredExecution, len(structuredSteps), unstructuredExecution, len(unstructuredSteps), unsupported
}

func mergeRunResult(target *ExecutionRunResult, incoming ExecutionRunResult) {
	if target.Artifacts == nil {
		target.Artifacts = map[string]string{}
	}
	for key, value := range incoming.Artifacts {
		target.Artifacts[key] = value
	}
	target.Notes = append(target.Notes, incoming.Notes...)
	target.ProcessedSteps += incoming.ProcessedSteps
	target.UsageSummary = mergeUsageSummary(target.UsageSummary, incoming.UsageSummary)
	target.StepHooks = append(target.StepHooks, incoming.StepHooks...)
}

func appendEngine(engines []string, engine string) []string {
	engine = strings.TrimSpace(engine)
	if engine == "" {
		return engines
	}
	for _, existing := range engines {
		if existing == engine {
			return engines
		}
	}
	return append(engines, engine)
}

func mergeUsageSummary(left, right map[string]any) map[string]any {
	if len(left) == 0 && len(right) == 0 {
		return nil
	}
	result := map[string]any{}
	for key, value := range left {
		result[key] = value
	}
	for key, value := range right {
		switch key {
		case "request_count", "input_tokens", "output_tokens", "total_tokens", "prompt_tokens", "input_text_count", "vector_count":
			result[key] = intValue(result[key]) + intValue(value)
		case "estimated_cost_usd":
			result[key] = roundCost(floatValue(result[key]) + floatValue(value))
		case "provider", "model", "operation", "cost_estimation_status":
			existing := strings.TrimSpace(stringValue(result[key]))
			incoming := strings.TrimSpace(stringValue(value))
			if existing == "" {
				result[key] = incoming
			} else if incoming == "" || existing == incoming {
				result[key] = existing
			} else {
				result[key] = "mixed"
			}
		default:
			if _, ok := result[key]; !ok {
				result[key] = value
			}
		}
	}
	return result
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

func floatValue(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	default:
		return 0
	}
}

func roundCost(value float64) float64 {
	return float64(int(value*100000000+0.5)) / 100000000
}
