package datasetprompts_test

import (
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/service/datasetprompts"
	"analysis-support-platform/control-plane/internal/store"
)

// ADR-015 Phase D1 lock — operator-only operation soft enforcement.
//
// Until auth lands the API checks the X-Operator-Mode header (mapped to
// CallerIsOperator on the request). For analyst-tier operations the
// header is irrelevant; for operator-only operations (planner /
// planner_meta) the header is required.
//
// silverone 2026-05-28 — 옛 service/prompt_access_test.go가 본 위치로 이동.

func newPromptAccessService(t *testing.T) (*datasetprompts.Service, store.Repository) {
	t.Helper()
	repo := store.NewMemoryStore()
	if err := repo.SaveProject(domain.Project{
		ProjectID: "p-access",
		Name:      "access",
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("save project: %v", err)
	}
	return datasetprompts.New(repo), repo
}

func TestPromptOperationEditableByMatchesAdr015Matrix(t *testing.T) {
	cases := map[string]string{
		"planner":                datasetprompts.PromptEditableByOperatorOnly,
		"planner_meta":           datasetprompts.PromptEditableByOperatorOnly,
		"prepare":                datasetprompts.PromptEditableByAnalyst,
		"sentiment":              datasetprompts.PromptEditableByAnalyst,
		"issue_evidence_summary": datasetprompts.PromptEditableByAnalyst,
		"execution_final_answer": datasetprompts.PromptEditableByAnalyst,
		"unknown_custom":         datasetprompts.PromptEditableByAnalyst,
	}
	for op, want := range cases {
		if got := datasetprompts.PromptOperationEditableBy(op); got != want {
			t.Fatalf("op=%s expected %q, got %q", op, want, got)
		}
	}
}

func TestSaveProjectPromptRejectsAnalystEditOfPlanner(t *testing.T) {
	svc, _ := newPromptAccessService(t)
	_, err := svc.SaveProjectPrompt("p-access", domain.ProjectPromptUpsertRequest{
		Version:      "planner-anthropic-v2",
		Operation:    "planner",
		Content:      "---\noperation: planner\n---\n{{allowed_skills}} {{active_layers}} {{skill_descriptions_block}} {{recommendations_block}} {{dataset_name}} {{dataset_version_id}} {{data_type}} {{goal}} {{constraints_json}} {{context_json}}\n",
		ChangeReason: "analyst tries to edit planner",
		// CallerIsOperator intentionally false (no X-Operator-Mode header).
	})
	if err == nil {
		t.Fatalf("expected operator_only rejection")
	}
	if !strings.Contains(err.Error(), "operator_only") {
		t.Fatalf("error should explain operator_only, got %v", err)
	}
}

func TestSaveProjectPromptAcceptsOperatorEditOfPlanner(t *testing.T) {
	svc, _ := newPromptAccessService(t)
	prompt, err := svc.SaveProjectPrompt("p-access", domain.ProjectPromptUpsertRequest{
		Version:          "planner-anthropic-v2",
		Operation:        "planner",
		Content:          "---\noperation: planner\n---\n{{allowed_skills}} {{active_layers}} {{skill_descriptions_block}} {{recommendations_block}} {{dataset_name}} {{dataset_version_id}} {{data_type}} {{goal}} {{constraints_json}} {{context_json}}\n",
		ChangeReason:     "operator iterates",
		CallerIsOperator: true,
	})
	if err != nil {
		t.Fatalf("operator edit should succeed: %v", err)
	}
	if prompt.Version != "planner-anthropic-v2" {
		t.Fatalf("unexpected version: %s", prompt.Version)
	}
}

func TestSaveProjectPromptAnalystCanEditAnalystOperation(t *testing.T) {
	svc, _ := newPromptAccessService(t)
	if _, err := svc.SaveProjectPrompt("p-access", domain.ProjectPromptUpsertRequest{
		Version:      "issue-evidence-summary-anthropic-v2",
		Operation:    "issue_evidence_summary",
		Content:      "---\noperation: issue_evidence_summary\n---\n{{dataset_name}} {{query}} {{analysis_context_json}} {{documents_json}}\n",
		ChangeReason: "analyst tweak",
		// CallerIsOperator false — analyst-tier operation, should still pass.
	}); err != nil {
		t.Fatalf("analyst edit of analyst-tier op should succeed: %v", err)
	}
}

func TestRevertProjectPromptRejectsAnalystForOperatorOnly(t *testing.T) {
	svc, _ := newPromptAccessService(t)
	// Set up two operator-mode versions first.
	for _, version := range []string{"planner-anthropic-v1", "planner-anthropic-v2"} {
		if _, err := svc.SaveProjectPrompt("p-access", domain.ProjectPromptUpsertRequest{
			Version:          version,
			Operation:        "planner",
			Content:          "---\noperation: planner\n---\n{{allowed_skills}} {{active_layers}} {{skill_descriptions_block}} {{recommendations_block}} {{dataset_name}} {{dataset_version_id}} {{data_type}} {{goal}} {{constraints_json}} {{context_json}}\n" + version + "\n",
			ChangeReason:     "operator setup",
			CallerIsOperator: true,
		}); err != nil {
			t.Fatalf("operator setup save (%s): %v", version, err)
		}
	}
	// Analyst attempts revert.
	_, err := svc.RevertProjectPrompt("p-access", "planner", domain.ProjectPromptRevertRequest{
		ToVersion:    "planner-anthropic-v1",
		NewVersion:   "planner-anthropic-v3",
		ChangeReason: "analyst tries to revert",
		// CallerIsOperator false.
	})
	if err == nil {
		t.Fatalf("expected operator_only rejection on revert")
	}
	if !strings.Contains(err.Error(), "operator_only") {
		t.Fatalf("error should explain operator_only, got %v", err)
	}
}
