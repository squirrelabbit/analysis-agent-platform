package skills

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"

	_ "github.com/marcboeker/go-duckdb"
)

type StructuredPlanResult struct {
	Artifacts      map[string]string `json:"artifacts"`
	Notes          []string          `json:"notes,omitempty"`
	ProcessedSteps int               `json:"processed_steps"`
	Engine         string            `json:"engine"`
	UsageSummary   map[string]any    `json:"usage_summary,omitempty"`
}

type ExecutionRunResult = StructuredPlanResult
type UnstructuredPlanResult = StructuredPlanResult

type ExecutionRunner interface {
	Run(ctx context.Context, execution domain.ExecutionSummary) (ExecutionRunResult, error)
}

type StructuredPlanRunner interface {
	Run(ctx context.Context, execution domain.ExecutionSummary) (ExecutionRunResult, error)
}

type UnstructuredPlanRunner interface {
	Run(ctx context.Context, execution domain.ExecutionSummary) (ExecutionRunResult, error)
}

type DuckDBRunner struct {
	Path string
	Open func(driverName, dataSourceName string) (*sql.DB, error)
}

type structuredKPISummaryArtifact struct {
	SkillName   string                     `json:"skill_name"`
	StepID      string                     `json:"step_id"`
	DatasetName string                     `json:"dataset_name"`
	Summary     structuredKPISummaryValues `json:"summary"`
	Series      []structuredSeriesPoint    `json:"series"`
}

type structuredKPISummaryValues struct {
	RowCount  int64   `json:"row_count"`
	MetricSum float64 `json:"metric_sum"`
	MetricAvg float64 `json:"metric_avg"`
	MetricMin float64 `json:"metric_min"`
	MetricMax float64 `json:"metric_max"`
}

type structuredSeriesPoint struct {
	Bucket string  `json:"bucket"`
	Value  float64 `json:"value"`
}

func (r DuckDBRunner) Run(ctx context.Context, execution domain.ExecutionSummary) (StructuredPlanResult, error) {
	openFn := r.Open
	if openFn == nil {
		openFn = sql.Open
	}

	path := strings.TrimSpace(r.Path)
	if path == "" {
		path = "analysis_support.duckdb"
	}

	db, err := openFn("duckdb", path)
	if err != nil {
		return StructuredPlanResult{}, err
	}
	defer db.Close()

	result := StructuredPlanResult{
		Artifacts: map[string]string{},
		Notes:     []string{},
		Engine:    "duckdb",
	}

	for _, step := range execution.Plan.Steps {
		switch step.SkillName {
		case "structured_kpi_summary":
			artifactJSON, err := r.runStructuredKPISummary(ctx, db, step)
			if err != nil {
				return StructuredPlanResult{}, fmt.Errorf("step %s (%s): %w", step.StepID, step.SkillName, err)
			}
			result.Artifacts[artifactKey(step)] = artifactJSON
			result.ProcessedSteps++
		default:
			result.Notes = append(result.Notes, fmt.Sprintf("unsupported skill skipped: %s", step.SkillName))
		}
	}

	if result.ProcessedSteps == 0 {
		result.Notes = append(result.Notes, "no structured skills were executed")
	}

	return result, nil
}

func artifactKey(step domain.SkillPlanStep) string {
	return fmt.Sprintf("step:%s:%s", step.StepID, step.SkillName)
}

func (r DuckDBRunner) runStructuredKPISummary(ctx context.Context, db *sql.DB, step domain.SkillPlanStep) (string, error) {
	timeColumn, err := inputString(step.Inputs, "time_column")
	if err != nil {
		return "", err
	}
	metricColumn, err := inputString(step.Inputs, "metric_column")
	if err != nil {
		return "", err
	}

	sourceSQL, err := relationSQL(step.DatasetName)
	if err != nil {
		return "", err
	}
	timeSQL, err := quoteIdentifier(timeColumn)
	if err != nil {
		return "", err
	}
	metricSQL, err := quoteIdentifier(metricColumn)
	if err != nil {
		return "", err
	}

	summaryQuery := fmt.Sprintf(
		`SELECT
			COUNT(*) AS row_count,
			COALESCE(SUM(%s), 0) AS metric_sum,
			COALESCE(AVG(%s), 0) AS metric_avg,
			COALESCE(MIN(%s), 0) AS metric_min,
			COALESCE(MAX(%s), 0) AS metric_max
		FROM %s`,
		metricSQL,
		metricSQL,
		metricSQL,
		metricSQL,
		sourceSQL,
	)

	var artifact structuredKPISummaryArtifact
	artifact.SkillName = step.SkillName
	artifact.StepID = step.StepID
	artifact.DatasetName = step.DatasetName
	if err := db.QueryRowContext(ctx, summaryQuery).Scan(
		&artifact.Summary.RowCount,
		&artifact.Summary.MetricSum,
		&artifact.Summary.MetricAvg,
		&artifact.Summary.MetricMin,
		&artifact.Summary.MetricMax,
	); err != nil {
		return "", err
	}

	seriesQuery := fmt.Sprintf(
		`SELECT CAST(%s AS VARCHAR) AS bucket, COALESCE(SUM(%s), 0) AS value
		FROM %s
		GROUP BY 1
		ORDER BY 1
		LIMIT 100`,
		timeSQL,
		metricSQL,
		sourceSQL,
	)

	rows, err := db.QueryContext(ctx, seriesQuery)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	for rows.Next() {
		var point structuredSeriesPoint
		if err := rows.Scan(&point.Bucket, &point.Value); err != nil {
			return "", err
		}
		artifact.Series = append(artifact.Series, point)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	payload, err := json.Marshal(artifact)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}

func inputString(inputs map[string]any, key string) (string, error) {
	if inputs == nil {
		return "", errors.New("skill inputs are required")
	}
	value, ok := inputs[key]
	if !ok {
		return "", fmt.Errorf("%s is required", key)
	}
	text := strings.TrimSpace(fmt.Sprintf("%v", value))
	if text == "" {
		return "", fmt.Errorf("%s is required", key)
	}
	return text, nil
}

func relationSQL(datasetName string) (string, error) {
	name := strings.TrimSpace(datasetName)
	if name == "" {
		return "", errors.New("dataset_name is required")
	}

	lower := strings.ToLower(name)
	switch {
	case strings.HasSuffix(lower, ".csv"):
		return fmt.Sprintf("read_csv_auto('%s')", escapeLiteral(name)), nil
	case strings.HasSuffix(lower, ".parquet"):
		return fmt.Sprintf("read_parquet('%s')", escapeLiteral(name)), nil
	default:
		return quoteQualifiedIdentifier(name)
	}
}

func quoteQualifiedIdentifier(value string) (string, error) {
	parts := strings.Split(value, ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return "", fmt.Errorf("invalid identifier: %s", value)
		}
		identifier, err := quoteIdentifier(part)
		if err != nil {
			return "", err
		}
		quoted = append(quoted, identifier)
	}
	return strings.Join(quoted, "."), nil
}

func quoteIdentifier(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("identifier is required")
	}
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`, nil
}

func escapeLiteral(value string) string {
	return strings.ReplaceAll(value, `'`, `''`)
}
