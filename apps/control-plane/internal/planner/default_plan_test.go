package planner

import (
	"reflect"
	"testing"

	"analysis-support-platform/control-plane/internal/registry"
)

func TestBuildDefaultPlanUsesBundleDefaults(t *testing.T) {
	input := testAnalysisInput()

	plan := BuildDefaultPlan(input)

	expectedSkills := registry.DefaultPlanSkills("unstructured")
	if len(plan.Steps) != len(expectedSkills) {
		t.Fatalf("unexpected step count: got %d want %d", len(plan.Steps), len(expectedSkills))
	}
	if plan.Notes == nil || *plan.Notes != defaultPlanNotes {
		t.Fatalf("unexpected notes: %+v", plan.Notes)
	}
	for index, skillName := range expectedSkills {
		step := plan.Steps[index]
		if step.SkillName != skillName {
			t.Fatalf("unexpected skill at %d: got %s want %s", index, step.SkillName, skillName)
		}
		if step.DatasetName != "issues.csv" {
			t.Fatalf("unexpected dataset name at %d: %s", index, step.DatasetName)
		}
		if !reflect.DeepEqual(step.Inputs, defaultInputsForSkill(skillName, input.Goal)) {
			t.Fatalf("unexpected inputs for %s: %+v", skillName, step.Inputs)
		}
	}
}

func TestBuildDefaultPlanFallsBackToStructuredDefaults(t *testing.T) {
	input := testAnalysisInput()
	input.DataType = nil
	input.DatasetName = nil

	plan := BuildDefaultPlan(input)

	expectedSkills := registry.DefaultPlanSkills("structured")
	if len(plan.Steps) != len(expectedSkills) {
		t.Fatalf("unexpected step count: got %d want %d", len(plan.Steps), len(expectedSkills))
	}
	if len(plan.Steps) == 0 {
		t.Fatalf("expected at least one default step")
	}
	if plan.Steps[0].DatasetName != DatasetFromVersion {
		t.Fatalf("expected placeholder dataset, got %s", plan.Steps[0].DatasetName)
	}
	if plan.Steps[0].SkillName != expectedSkills[0] {
		t.Fatalf("unexpected first skill: got %s want %s", plan.Steps[0].SkillName, expectedSkills[0])
	}
}
