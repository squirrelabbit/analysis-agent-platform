package datasetprompts_test

import (
	"strings"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/service/datasetprompts"
	"analysis-support-platform/control-plane/internal/store"
)

// ADR-015 Phase C lock — audit log invariants.
//
//  1. ``change_reason`` is mandatory on prompt mutations.
//  2. Every successful create/revert appends exactly one
//     ``ProjectPromptChange`` row.
//  3. revert clones the source body into a *new* version (never mutates
//     the active row in place — Codex review §Q4).
//  4. The diff helper reports correct add/remove line counts.
//
// silverone 2026-05-28 — 옛 service/prompt_audit_test.go가 본 위치로 이동.

func newPromptAuditService(t *testing.T) (*datasetprompts.Service, store.Repository) {
	t.Helper()
	repo := store.NewMemoryStore()
	if err := repo.SaveProject(domain.Project{
		ProjectID: "p-audit",
		Name:      "audit",
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("save project: %v", err)
	}
	return datasetprompts.New(repo), repo
}

func TestSaveProjectPromptRequiresChangeReason(t *testing.T) {
	svc, _ := newPromptAuditService(t)
	_, err := svc.SaveProjectPrompt("p-audit", domain.ProjectPromptUpsertRequest{
		Version:   "project-prepare-v1",
		Operation: "issue_evidence_summary",
		Content:   "---\noperation: issue_evidence_summary\n---\n{{dataset_name}} {{query}} {{analysis_context_json}} {{documents_json}}\n",
		// ChangeReason intentionally empty.
	})
	if err == nil {
		t.Fatalf("expected change_reason required error")
	}
	if !strings.Contains(err.Error(), "change_reason") {
		t.Fatalf("error does not mention change_reason: %v", err)
	}
}

func TestSaveProjectPromptAppendsCreateAuditRow(t *testing.T) {
	svc, repo := newPromptAuditService(t)
	if _, err := svc.SaveProjectPrompt("p-audit", domain.ProjectPromptUpsertRequest{
		Version:      "project-prepare-v1",
		Operation:    "issue_evidence_summary",
		Content:      "---\noperation: issue_evidence_summary\n---\n{{dataset_name}} {{query}} {{analysis_context_json}} {{documents_json}}\n",
		ChangeReason: "first version",
	}); err != nil {
		t.Fatalf("save: %v", err)
	}
	changes, err := repo.ListProjectPromptChanges("p-audit", "issue_evidence_summary")
	if err != nil {
		t.Fatalf("list changes: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected exactly one create row, got %d", len(changes))
	}
	if changes[0].Action != "create" || changes[0].ChangeReason != "first version" {
		t.Fatalf("unexpected audit row: %+v", changes[0])
	}
	if changes[0].PreviousContentHash != "" || changes[0].NewContentHash == "" {
		t.Fatalf("create row should have empty previous + non-empty new hash: %+v", changes[0])
	}
}

func TestRevertProjectPromptCreatesNewVersionAndAppendsRevertRow(t *testing.T) {
	svc, repo := newPromptAuditService(t)
	if _, err := svc.SaveProjectPrompt("p-audit", domain.ProjectPromptUpsertRequest{
		Version:      "v1",
		Operation:    "issue_evidence_summary",
		Content:      "---\noperation: issue_evidence_summary\n---\n{{dataset_name}} {{query}} {{analysis_context_json}} {{documents_json}}\nold body\n",
		ChangeReason: "initial",
	}); err != nil {
		t.Fatalf("save v1: %v", err)
	}
	if _, err := svc.SaveProjectPrompt("p-audit", domain.ProjectPromptUpsertRequest{
		Version:      "v2",
		Operation:    "issue_evidence_summary",
		Content:      "---\noperation: issue_evidence_summary\n---\n{{dataset_name}} {{query}} {{analysis_context_json}} {{documents_json}}\nnew body\n",
		ChangeReason: "iterate",
	}); err != nil {
		t.Fatalf("save v2: %v", err)
	}

	revert, err := svc.RevertProjectPrompt("p-audit", "issue_evidence_summary", domain.ProjectPromptRevertRequest{
		ToVersion:    "v1",
		NewVersion:   "v3",
		ChangeReason: "v2 was bad",
	})
	if err != nil {
		t.Fatalf("revert: %v", err)
	}
	if revert.Version != "v3" {
		t.Fatalf("expected new version v3, got %s", revert.Version)
	}
	if !strings.Contains(revert.Content, "old body") {
		t.Fatalf("revert body should match v1 source, got %q", revert.Content)
	}

	// v1 and v2 must still exist unmodified (revert is non-mutating).
	if _, err := repo.GetProjectPrompt("p-audit", "v1", "issue_evidence_summary"); err != nil {
		t.Fatalf("v1 must remain after revert: %v", err)
	}
	if _, err := repo.GetProjectPrompt("p-audit", "v2", "issue_evidence_summary"); err != nil {
		t.Fatalf("v2 must remain after revert: %v", err)
	}

	changes, err := repo.ListProjectPromptChanges("p-audit", "issue_evidence_summary")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(changes) != 3 {
		t.Fatalf("expected 3 audit rows, got %d", len(changes))
	}
	last := changes[len(changes)-1]
	if last.Action != "revert" {
		t.Fatalf("last action should be revert, got %q", last.Action)
	}
	if last.BaseVersion != "v1" {
		t.Fatalf("revert row should reference v1 as base, got %q", last.BaseVersion)
	}
	if last.ChangeReason != "v2 was bad" {
		t.Fatalf("revert reason mismatch: %q", last.ChangeReason)
	}
}

func TestRevertProjectPromptRejectsExistingNewVersion(t *testing.T) {
	svc, _ := newPromptAuditService(t)
	if _, err := svc.SaveProjectPrompt("p-audit", domain.ProjectPromptUpsertRequest{
		Version:      "v1",
		Operation:    "issue_evidence_summary",
		Content:      "---\noperation: issue_evidence_summary\n---\n{{dataset_name}} {{query}} {{analysis_context_json}} {{documents_json}}\n",
		ChangeReason: "create",
	}); err != nil {
		t.Fatalf("save v1: %v", err)
	}
	if _, err := svc.SaveProjectPrompt("p-audit", domain.ProjectPromptUpsertRequest{
		Version:      "v2",
		Operation:    "issue_evidence_summary",
		Content:      "---\noperation: issue_evidence_summary\n---\n{{dataset_name}} {{query}} {{analysis_context_json}} {{documents_json}}\nB\n",
		ChangeReason: "create",
	}); err != nil {
		t.Fatalf("save v2: %v", err)
	}
	_, err := svc.RevertProjectPrompt("p-audit", "issue_evidence_summary", domain.ProjectPromptRevertRequest{
		ToVersion:    "v1",
		NewVersion:   "v2", // already exists
		ChangeReason: "should fail",
	})
	if err == nil {
		t.Fatalf("expected conflict on revert into existing version")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("error should mention existing version, got %v", err)
	}
}

func TestComputeUnifiedDiffReportsLineStats(t *testing.T) {
	base := "alpha\nbeta\ngamma\ndelta\n"
	head := "alpha\nBETA\ngamma\nepsilon\n"
	diff, stats := datasetprompts.ComputeUnifiedDiff(base, head)

	if stats.AddedLines != 2 {
		t.Fatalf("expected 2 added lines, got %d (%q)", stats.AddedLines, diff)
	}
	if stats.RemovedLines != 2 {
		t.Fatalf("expected 2 removed lines, got %d (%q)", stats.RemovedLines, diff)
	}
	if !strings.Contains(diff, "- beta") || !strings.Contains(diff, "+ BETA") {
		t.Fatalf("diff did not capture beta replacement: %q", diff)
	}
}
