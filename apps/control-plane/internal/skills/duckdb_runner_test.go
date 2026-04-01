package skills

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"analysis-support-platform/control-plane/internal/domain"

	_ "github.com/marcboeker/go-duckdb"
)

func TestDuckDBRunnerBuildsStructuredKPISummaryArtifact(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "analysis.duckdb")

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("unexpected open error: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE sales_kpi ("date" DATE, "value" DOUBLE);
		INSERT INTO sales_kpi VALUES
			('2026-03-01', 10),
			('2026-03-02', 15),
			('2026-03-03', 5);
	`); err != nil {
		_ = db.Close()
		t.Fatalf("unexpected setup error: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("unexpected close error: %v", err)
	}

	runner := DuckDBRunner{Path: dbPath}
	result, err := runner.Run(context.Background(), domain.ExecutionSummary{
		ExecutionID: "exec-1",
		Plan: domain.SkillPlan{
			PlanID: "plan-1",
			Steps: []domain.SkillPlanStep{
				{
					StepID:      "step-1",
					SkillName:   "structured_kpi_summary",
					DatasetName: "sales_kpi",
					Inputs: map[string]any{
						"time_column":   "date",
						"metric_column": "value",
					},
				},
			},
			CreatedAt: time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC),
		},
	})
	if err != nil {
		t.Fatalf("unexpected run error: %v", err)
	}

	if result.Engine != "duckdb" {
		t.Fatalf("unexpected engine: %s", result.Engine)
	}
	if result.ProcessedSteps != 1 {
		t.Fatalf("unexpected processed steps: %d", result.ProcessedSteps)
	}
	if len(result.StepHooks) != 2 {
		t.Fatalf("unexpected step hook count: %+v", result.StepHooks)
	}
	if result.StepHooks[0].Phase != "before" || result.StepHooks[1].Phase != "after" {
		t.Fatalf("unexpected step hooks: %+v", result.StepHooks)
	}

	rawArtifact, ok := result.Artifacts["step:step-1:structured_kpi_summary"]
	if !ok {
		t.Fatalf("artifact not found: %+v", result.Artifacts)
	}

	var artifact structuredKPISummaryArtifact
	if err := json.Unmarshal([]byte(rawArtifact), &artifact); err != nil {
		t.Fatalf("unexpected artifact decode error: %v", err)
	}

	if artifact.Summary.RowCount != 3 {
		t.Fatalf("unexpected row count: %d", artifact.Summary.RowCount)
	}
	if artifact.Summary.MetricSum != 30 {
		t.Fatalf("unexpected metric sum: %v", artifact.Summary.MetricSum)
	}
	if artifact.Summary.MetricAvg != 10 {
		t.Fatalf("unexpected metric avg: %v", artifact.Summary.MetricAvg)
	}
	if len(artifact.Series) != 3 {
		t.Fatalf("unexpected series length: %d", len(artifact.Series))
	}
	if status, ok := result.StepHooks[1].Payload["status"].(string); !ok || status != "completed" {
		t.Fatalf("unexpected after hook payload: %+v", result.StepHooks[1].Payload)
	}
}
