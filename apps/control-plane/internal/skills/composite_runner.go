package skills

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/registry"
)

type CompositeRunner struct {
	Structured   StructuredPlanRunner
	Unstructured UnstructuredPlanRunner
}

func (r CompositeRunner) Run(ctx context.Context, execution domain.ExecutionSummary) (ExecutionRunResult, error) {
	structuredExecution, structuredCount, unstructuredExecution, unstructuredCount, unsupported := splitExecution(execution)

	result := ExecutionRunResult{
		Artifacts: map[string]string{},
		Notes:     []string{},
	}
	engines := []string{}

	if structuredCount > 0 {
		if r.Structured == nil {
			return ExecutionRunResult{}, errors.New("structured runner is required")
		}
		runResult, err := r.Structured.Run(ctx, structuredExecution)
		if err != nil {
			return ExecutionRunResult{}, err
		}
		mergeRunResult(&result, runResult)
		engines = appendEngine(engines, runResult.Engine)
	}

	if unstructuredCount > 0 {
		if r.Unstructured == nil {
			return ExecutionRunResult{}, errors.New("unstructured runner is required")
		}
		runResult, err := r.Unstructured.Run(ctx, unstructuredExecution)
		if err != nil {
			return ExecutionRunResult{}, err
		}
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
