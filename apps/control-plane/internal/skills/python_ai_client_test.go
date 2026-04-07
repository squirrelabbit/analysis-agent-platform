package skills

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

func TestPythonAIClientRunsUnstructuredTasks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}

		switch r.URL.Path {
		case "/tasks/issue_breakdown_summary":
			if !strings.Contains(string(body), `"dimension_column":"channel"`) {
				t.Fatalf("unexpected breakdown request body: %s", string(body))
			}
			_, _ = w.Write([]byte(`{
				"notes":["breakdown path completed"],
				"artifact":{
					"skill_name":"issue_breakdown_summary",
					"usage":{"provider":"anthropic","model":"claude-sonnet-4-6","operation":"issue_breakdown_summary","request_count":1,"input_tokens":100,"output_tokens":20,"total_tokens":120,"cost_estimation_status":"not_configured"},
					"summary":{"group_count":2,"top_group":"app"},
					"breakdown":[{"dimension_value":"app","count":2}]
				}
			}`))
		case "/tasks/issue_period_compare":
			if !strings.Contains(string(body), `"window_size":1`) {
				t.Fatalf("unexpected compare request body: %s", string(body))
			}
			_, _ = w.Write([]byte(`{
				"notes":["compare path completed"],
				"artifact":{
					"skill_name":"issue_period_compare",
					"summary":{"current_count":3,"previous_count":1,"count_delta":2},
					"periods":{"current":{"start_bucket":"2026-03-27"},"previous":{"start_bucket":"2026-03-26"}}
				}
			}`))
		case "/tasks/issue_sentiment_summary":
			if !strings.Contains(string(body), `"sentiment_column":"sentiment_label"`) {
				t.Fatalf("unexpected sentiment request body: %s", string(body))
			}
			if !strings.Contains(string(body), `"prepared_dataset_name":"/tmp/issues.prepared.parquet"`) {
				t.Fatalf("unexpected prepared dataset in sentiment request body: %s", string(body))
			}
			_, _ = w.Write([]byte(`{
				"notes":["sentiment path completed"],
				"artifact":{
					"skill_name":"issue_sentiment_summary",
					"summary":{"document_count":4,"dominant_label":"negative","negative_count":2},
					"breakdown":[{"sentiment_label":"negative","count":2}]
				}
			}`))
		case "/tasks/issue_trend_summary":
			if !strings.Contains(string(body), `"time_column":"occurred_at"`) {
				t.Fatalf("unexpected trend request body: %s", string(body))
			}
			_, _ = w.Write([]byte(`{
				"notes":["trend path completed"],
				"artifact":{
					"skill_name":"issue_trend_summary",
					"summary":{"bucket_count":3,"peak_bucket":"2026-03-24"},
					"series":[{"bucket":"2026-03-24","count":2}]
				}
			}`))
		case "/tasks/unstructured_issue_summary":
			if !strings.Contains(string(body), `"skill_name":"unstructured_issue_summary"`) {
				t.Fatalf("unexpected request body: %s", string(body))
			}
			_, _ = w.Write([]byte(`{
				"notes":["python path completed"],
				"artifact":{
					"skill_name":"unstructured_issue_summary",
					"usage":{"provider":"anthropic","model":"claude-sonnet-4-6","operation":"unstructured_issue_summary","request_count":1,"input_tokens":80,"output_tokens":10,"total_tokens":90,"cost_estimation_status":"not_configured"},
					"summary":{"document_count":2},
					"top_terms":[{"term":"error","count":3}]
				}
			}`))
		case "/tasks/semantic_search":
			if !strings.Contains(string(body), `"query":"관련 문서를 찾아줘"`) {
				t.Fatalf("unexpected semantic search request body: %s", string(body))
			}
			if !strings.Contains(string(body), `"dataset_version_id":"version-1"`) {
				t.Fatalf("unexpected semantic search dataset version in request body: %s", string(body))
			}
			if !strings.Contains(string(body), `"chunk_ref":"/tmp/issues.chunks.parquet"`) {
				t.Fatalf("unexpected semantic search chunk ref in request body: %s", string(body))
			}
			_, _ = w.Write([]byte(`{
				"notes":["semantic search completed"],
				"artifact":{
					"skill_name":"semantic_search",
					"citation_mode":"chunk",
					"chunk_ref":"/tmp/issues.chunks.parquet",
					"summary":{"match_count":2,"citation_mode":"chunk"},
					"matches":[{"rank":1,"score":0.9,"text":"결제 오류","chunk_id":"row-1:chunk-0","char_start":0,"char_end":5}]
				}
			}`))
		case "/tasks/evidence_pack":
			if !strings.Contains(string(body), `"query":"VOC 이슈를 요약해줘"`) {
				t.Fatalf("unexpected evidence request body: %s", string(body))
			}
			if !strings.Contains(string(body), `"step:step-0:issue_trend_summary":{`) {
				t.Fatalf("expected issue_trend_summary artifact in evidence request body: %s", string(body))
			}
			if !strings.Contains(string(body), `"step:step-1:unstructured_issue_summary":{`) {
				t.Fatalf("expected prior artifacts in evidence request body: %s", string(body))
			}
			if !strings.Contains(string(body), `"step:step-2:semantic_search":{`) {
				t.Fatalf("expected semantic_search artifact in evidence request body: %s", string(body))
			}
			if !strings.Contains(string(body), `"skill_name":"semantic_search"`) {
				t.Fatalf("expected semantic_search artifact in evidence request body: %s", string(body))
			}
			_, _ = w.Write([]byte(`{
				"notes":["evidence path completed"],
				"artifact":{
					"skill_name":"evidence_pack",
					"selection_source":"semantic_search",
					"citation_mode":"chunk",
					"chunk_ref":"/tmp/issues.chunks.parquet",
					"summary":"대표 이슈 근거를 모았습니다",
					"evidence":[{"rank":1,"source_index":0,"snippet":"결제 오류","rationale":"selected","chunk_id":"row-1:chunk-0","char_start":0,"char_end":5}]
				}
			}`))
		case "/tasks/issue_evidence_summary":
			if !strings.Contains(string(body), `"query":"VOC 이슈를 요약해줘"`) {
				t.Fatalf("unexpected issue evidence request body: %s", string(body))
			}
			if !strings.Contains(string(body), `"step:step-00:issue_period_compare":{`) {
				t.Fatalf("expected issue_period_compare artifact in issue evidence request body: %s", string(body))
			}
			if !strings.Contains(string(body), `"step:step-0:issue_trend_summary":{`) {
				t.Fatalf("expected issue_trend_summary artifact in issue evidence request body: %s", string(body))
			}
			if !strings.Contains(string(body), `"step:step-1:issue_breakdown_summary":{`) {
				t.Fatalf("expected issue_breakdown_summary artifact in issue evidence request body: %s", string(body))
			}
			if !strings.Contains(string(body), `"step:step-4:semantic_search":{`) {
				t.Fatalf("expected semantic_search artifact in issue evidence request body: %s", string(body))
			}
			_, _ = w.Write([]byte(`{
				"notes":["issue evidence path completed"],
				"artifact":{
					"skill_name":"issue_evidence_summary",
					"selection_source":"semantic_search",
					"citation_mode":"chunk",
					"chunk_ref":"/tmp/issues.chunks.parquet",
					"summary":"대표 이슈 근거를 모았습니다",
					"evidence":[{"rank":1,"source_index":0,"snippet":"결제 오류","rationale":"selected","chunk_id":"row-1:chunk-0","char_start":0,"char_end":5}]
				}
			}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := PythonAIClient{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	}
	datasetVersionID := "version-1"

	result, err := client.Run(context.Background(), domain.ExecutionSummary{
		ExecutionID:      "exec-1",
		ProjectID:        "project-1",
		DatasetVersionID: &datasetVersionID,
		Plan: domain.SkillPlan{
			Steps: []domain.SkillPlanStep{
				{
					StepID:      "step-00",
					SkillName:   "issue_period_compare",
					DatasetName: "/tmp/issues_compare.csv",
					Inputs: map[string]any{
						"text_column": "text",
						"time_column": "occurred_at",
						"bucket":      "day",
						"window_size": 1,
					},
				},
				{
					StepID:      "step-0",
					SkillName:   "issue_trend_summary",
					DatasetName: "/tmp/issues_trend.csv",
					Inputs: map[string]any{
						"text_column": "text",
						"time_column": "occurred_at",
						"bucket":      "day",
					},
				},
				{
					StepID:      "step-1",
					SkillName:   "issue_breakdown_summary",
					DatasetName: "/tmp/issues_breakdown.csv",
					Inputs: map[string]any{
						"text_column":      "text",
						"dimension_column": "channel",
					},
				},
				{
					StepID:      "step-2",
					SkillName:   "issue_sentiment_summary",
					DatasetName: "/tmp/issues.sentiment.parquet",
					Inputs: map[string]any{
						"text_column":           "normalized_text",
						"sentiment_column":      "sentiment_label",
						"prepared_dataset_name": "/tmp/issues.prepared.parquet",
					},
				},
				{
					StepID:      "step-3",
					SkillName:   "unstructured_issue_summary",
					DatasetName: "/tmp/issues.csv",
					Inputs: map[string]any{
						"text_column": "text",
					},
				},
				{
					StepID:      "step-4",
					SkillName:   "semantic_search",
					DatasetName: "/tmp/issues.csv",
					Inputs: map[string]any{
						"text_column":   "text",
						"sample_n":      3,
						"query":         "관련 문서를 찾아줘",
						"embedding_uri": "/tmp/issues.csv.embeddings.jsonl",
						"chunk_ref":     "/tmp/issues.chunks.parquet",
					},
				},
				{
					StepID:      "step-5",
					SkillName:   "issue_evidence_summary",
					DatasetName: "/tmp/issues.csv",
					Inputs: map[string]any{
						"text_column": "text",
						"sample_n":    3,
						"query":       "VOC 이슈를 요약해줘",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Engine != "python-ai" {
		t.Fatalf("unexpected engine: %s", result.Engine)
	}
	if result.ProcessedSteps != 7 {
		t.Fatalf("unexpected processed steps: %d", result.ProcessedSteps)
	}
	if len(result.StepHooks) != 14 {
		t.Fatalf("unexpected step hook count: %+v", result.StepHooks)
	}
	if result.StepHooks[0].Phase != "before" || result.StepHooks[0].StepID != "step-00" {
		t.Fatalf("unexpected first step hook: %+v", result.StepHooks[0])
	}
	if result.StepHooks[1].Phase != "after" {
		t.Fatalf("unexpected second step hook: %+v", result.StepHooks[1])
	}
	if len(result.Notes) != 7 {
		t.Fatalf("unexpected notes: %+v", result.Notes)
	}
	compareArtifact := result.Artifacts["step:step-00:issue_period_compare"]
	if !strings.Contains(compareArtifact, `"count_delta":2`) {
		t.Fatalf("unexpected compare artifact: %s", compareArtifact)
	}
	trendArtifact := result.Artifacts["step:step-0:issue_trend_summary"]
	if !strings.Contains(trendArtifact, `"peak_bucket":"2026-03-24"`) {
		t.Fatalf("unexpected trend artifact: %s", trendArtifact)
	}
	breakdownArtifact := result.Artifacts["step:step-1:issue_breakdown_summary"]
	if !strings.Contains(breakdownArtifact, `"top_group":"app"`) {
		t.Fatalf("unexpected breakdown artifact: %s", breakdownArtifact)
	}
	sentimentArtifact := result.Artifacts["step:step-2:issue_sentiment_summary"]
	if !strings.Contains(sentimentArtifact, `"dominant_label":"negative"`) {
		t.Fatalf("unexpected sentiment artifact: %s", sentimentArtifact)
	}
	artifact := result.Artifacts["step:step-3:unstructured_issue_summary"]
	if !strings.Contains(artifact, `"document_count":2`) {
		t.Fatalf("unexpected artifact: %s", artifact)
	}
	searchArtifact := result.Artifacts["step:step-4:semantic_search"]
	if !strings.Contains(searchArtifact, `"match_count":2`) {
		t.Fatalf("unexpected semantic artifact: %s", searchArtifact)
	}
	if !strings.Contains(searchArtifact, `"chunk_ref":"/tmp/issues.chunks.parquet"`) {
		t.Fatalf("unexpected semantic artifact: %s", searchArtifact)
	}
	evidenceArtifact := result.Artifacts["step:step-5:issue_evidence_summary"]
	if !strings.Contains(evidenceArtifact, `"selection_source":"semantic_search"`) {
		t.Fatalf("unexpected evidence artifact: %s", evidenceArtifact)
	}
	if !strings.Contains(evidenceArtifact, `"chunk_id":"row-1:chunk-0"`) {
		t.Fatalf("unexpected evidence artifact: %s", evidenceArtifact)
	}
	if !strings.Contains(evidenceArtifact, `"summary":"대표 이슈 근거를 모았습니다"`) {
		t.Fatalf("unexpected evidence artifact: %s", evidenceArtifact)
	}
}

func TestPythonAIClientRunsSupportTasks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/tasks/document_filter":
			_, _ = w.Write([]byte(`{
				"notes":["filter path completed"],
				"artifact":{
					"skill_name":"document_filter",
					"usage":{"provider":"anthropic","model":"claude-haiku","operation":"document_filter","request_count":1,"input_tokens":80,"output_tokens":40,"total_tokens":120,"cost_estimation_status":"not_configured"},
					"matched_indices":[0,2],
					"summary":{"filtered_row_count":2}
				}
			}`))
		case "/tasks/keyword_frequency":
			_, _ = w.Write([]byte(`{
				"notes":["keyword path completed"],
				"artifact":{
					"skill_name":"keyword_frequency",
					"usage":{"provider":"anthropic","model":"claude-haiku","operation":"keyword_frequency","request_count":1,"input_tokens":60,"output_tokens":30,"total_tokens":90,"cost_estimation_status":"not_configured"},
					"summary":{"document_count":2},
					"top_terms":[{"term":"결제","count":2}]
				}
			}`))
		case "/tasks/noun_frequency":
			_, _ = w.Write([]byte(`{
				"notes":["noun path completed"],
				"artifact":{
					"skill_name":"noun_frequency",
					"usage":{"provider":"local","model":"kiwi","operation":"noun_frequency","request_count":1,"input_tokens":0,"output_tokens":0,"total_tokens":0,"cost_estimation_status":"free"},
					"summary":{"document_count":2,"analyzer_backend":"kiwi"},
					"top_nouns":[{"term":"결제","term_frequency":2,"document_frequency":2}]
				}
			}`))
		case "/tasks/time_bucket_count":
			_, _ = w.Write([]byte(`{
				"notes":["time bucket path completed"],
				"artifact":{"skill_name":"time_bucket_count","summary":{"bucket_count":2,"peak_bucket":"2026-03-24"},"series":[{"bucket":"2026-03-24","count":2}]}
			}`))
		case "/tasks/meta_group_count":
			_, _ = w.Write([]byte(`{
				"notes":["meta group path completed"],
				"artifact":{"skill_name":"meta_group_count","summary":{"top_group":"app"},"breakdown":[{"dimension_value":"app","count":2}]}
			}`))
		case "/tasks/document_sample":
			_, _ = w.Write([]byte(`{
				"notes":["sample path completed"],
				"artifact":{"skill_name":"document_sample","summary":{"sample_count":2},"samples":[{"rank":1,"source_index":0,"score":2,"text":"결제 오류"}]}
			}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := PythonAIClient{
		BaseURL:    server.URL,
		HTTPClient: server.Client(),
	}

	result, err := client.Run(context.Background(), domain.ExecutionSummary{
		ExecutionID: "exec-support",
		ProjectID:   "project-1",
		Plan: domain.SkillPlan{
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "document_filter", DatasetName: "/tmp/issues.csv", Inputs: map[string]any{"text_column": "text", "query": "결제 오류"}},
				{StepID: "step-2", SkillName: "keyword_frequency", DatasetName: "/tmp/issues.csv", Inputs: map[string]any{"text_column": "text", "top_n": 3}},
				{StepID: "step-3", SkillName: "noun_frequency", DatasetName: "/tmp/issues.csv", Inputs: map[string]any{"text_column": "text", "top_n": 3}},
				{StepID: "step-4", SkillName: "time_bucket_count", DatasetName: "/tmp/issues.csv", Inputs: map[string]any{"text_column": "text", "time_column": "occurred_at", "bucket": "day"}},
				{StepID: "step-5", SkillName: "meta_group_count", DatasetName: "/tmp/issues.csv", Inputs: map[string]any{"text_column": "text", "dimension_column": "channel"}},
				{StepID: "step-6", SkillName: "document_sample", DatasetName: "/tmp/issues.csv", Inputs: map[string]any{"text_column": "text", "query": "결제 오류"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ProcessedSteps != 6 {
		t.Fatalf("unexpected processed steps: %d", result.ProcessedSteps)
	}
	if !strings.Contains(result.Artifacts["step:step-2:keyword_frequency"], `"결제"`) {
		t.Fatalf("unexpected keyword artifact: %s", result.Artifacts["step:step-2:keyword_frequency"])
	}
	if !strings.Contains(result.Artifacts["step:step-3:noun_frequency"], `"analyzer_backend":"kiwi"`) {
		t.Fatalf("unexpected noun artifact: %s", result.Artifacts["step:step-3:noun_frequency"])
	}
	if !strings.Contains(result.Artifacts["step:step-6:document_sample"], `"sample_count":2`) {
		t.Fatalf("unexpected sample artifact: %s", result.Artifacts["step:step-6:document_sample"])
	}
	if result.UsageSummary == nil {
		t.Fatalf("expected usage summary")
	}
	if totalTokens, ok := result.UsageSummary["total_tokens"].(int); !ok || totalTokens != 210 {
		t.Fatalf("unexpected usage summary: %+v", result.UsageSummary)
	}
	if len(result.StepHooks) != 12 {
		t.Fatalf("unexpected step hook count: %+v", result.StepHooks)
	}
}

func TestPythonAIClientStoresGarbageFilterAsSidecarRefButKeepsRuntimeArtifactForNextStep(t *testing.T) {
	artifactRoot := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
		switch r.URL.Path {
		case "/tasks/garbage_filter":
			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("unexpected unmarshal error: %v", err)
			}
			step := payload["step"].(map[string]any)
			inputs := step["inputs"].(map[string]any)
			outputPath := inputs["artifact_output_path"].(string)
			expectedSuffix := filepath.Join("steps", "step-1.garbage_filter.rows.parquet")
			if !strings.HasPrefix(outputPath, artifactRoot) || !strings.HasSuffix(outputPath, expectedSuffix) {
				t.Fatalf("unexpected garbage_filter output path: %s", outputPath)
			}
			if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
				t.Fatalf("unexpected mkdir error: %v", err)
			}
			if err := os.WriteFile(outputPath, []byte("parquet-placeholder"), 0o644); err != nil {
				t.Fatalf("unexpected write error: %v", err)
			}
			_, _ = w.Write([]byte(`{
				"notes":["garbage path completed"],
				"artifact":{
					"skill_name":"garbage_filter",
					"summary":{"input_row_count":4,"retained_row_count":2,"removed_row_count":2,"garbage_rule_hits":{"ad_marker":1}},
					"retained_indices":[1,2],
					"removed_indices":[0,3],
					"removed_samples":[{"source_index":0,"matched_rules":["ad_marker"],"text":"광고 문구"}],
					"artifact_storage_mode":"sidecar_ref",
					"artifact_ref":"` + outputPath + `",
					"artifact_format":"parquet",
					"row_id_column":"row_id",
					"source_index_column":"source_index",
					"status_column":"filter_status",
					"matched_rules_column":"matched_rules"
				}
			}`))
		case "/tasks/document_filter":
			if !strings.Contains(string(body), `"artifact_ref":"`) {
				t.Fatalf("expected compacted garbage_filter artifact_ref in prior artifacts, got: %s", string(body))
			}
			if strings.Contains(string(body), `"retained_indices":[1,2]`) {
				t.Fatalf("expected runtime garbage_filter artifact to omit retained_indices, got: %s", string(body))
			}
			_, _ = w.Write([]byte(`{
				"notes":["document filter completed"],
				"artifact":{
					"skill_name":"document_filter",
					"summary":{"filtered_row_count":2},
					"matched_indices":[1,2]
				}
			}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := PythonAIClient{
		BaseURL:      server.URL,
		HTTPClient:   server.Client(),
		ArtifactRoot: artifactRoot,
	}

	result, err := client.Run(context.Background(), domain.ExecutionSummary{
		ExecutionID: "exec-garbage",
		ProjectID:   "project-1",
		Plan: domain.SkillPlan{
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "garbage_filter", DatasetName: "/tmp/issues.prepared.parquet", Inputs: map[string]any{"text_column": "normalized_text"}},
				{StepID: "step-2", SkillName: "document_filter", DatasetName: "/tmp/issues.prepared.parquet", Inputs: map[string]any{"text_column": "normalized_text", "query": "결제 오류"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	garbageArtifact := result.Artifacts["step:step-1:garbage_filter"]
	if !strings.Contains(garbageArtifact, `"artifact_ref":"`) {
		t.Fatalf("expected artifact_ref in stored garbage artifact: %s", garbageArtifact)
	}
	if strings.Contains(garbageArtifact, `"retained_indices"`) {
		t.Fatalf("stored garbage artifact should be compacted: %s", garbageArtifact)
	}
	if !strings.Contains(result.Artifacts["step:step-2:document_filter"], `"filtered_row_count":2`) {
		t.Fatalf("unexpected document_filter artifact: %s", result.Artifacts["step:step-2:document_filter"])
	}
}

func TestPythonAIClientStoresDocumentFilterAsSidecarRefButKeepsRuntimeArtifactForNextStep(t *testing.T) {
	artifactRoot := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
		switch r.URL.Path {
		case "/tasks/document_filter":
			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("unexpected unmarshal error: %v", err)
			}
			step := payload["step"].(map[string]any)
			inputs := step["inputs"].(map[string]any)
			outputPath := inputs["artifact_output_path"].(string)
			expectedSuffix := filepath.Join("steps", "step-1.document_filter.matches.parquet")
			if !strings.HasPrefix(outputPath, artifactRoot) || !strings.HasSuffix(outputPath, expectedSuffix) {
				t.Fatalf("unexpected document_filter output path: %s", outputPath)
			}
			if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
				t.Fatalf("unexpected mkdir error: %v", err)
			}
			if err := os.WriteFile(outputPath, []byte("parquet-placeholder"), 0o644); err != nil {
				t.Fatalf("unexpected write error: %v", err)
			}
			_, _ = w.Write([]byte(`{
				"notes":["document filter completed"],
				"artifact":{
					"skill_name":"document_filter",
					"query":"결제 오류",
					"summary":{"input_row_count":4,"filtered_row_count":2,"selection_mode":"lexical_overlap","query_token_count":2},
					"matched_indices":[1,2],
					"matches":[{"rank":1,"source_index":1,"score":2,"text":"결제 오류"},{"rank":2,"source_index":2,"score":1,"text":"결제 승인 오류"}],
					"artifact_storage_mode":"sidecar_ref",
					"artifact_ref":"` + outputPath + `",
					"artifact_format":"parquet",
					"row_id_column":"row_id",
					"source_index_column":"source_index",
					"rank_column":"rank",
					"score_column":"score"
				}
			}`))
		case "/tasks/keyword_frequency":
			if !strings.Contains(string(body), `"artifact_ref":"`) {
				t.Fatalf("expected compacted document_filter artifact_ref in prior artifacts, got: %s", string(body))
			}
			if strings.Contains(string(body), `"matched_indices":[1,2]`) {
				t.Fatalf("expected runtime document_filter artifact to omit matched_indices, got: %s", string(body))
			}
			_, _ = w.Write([]byte(`{
				"notes":["keyword completed"],
				"artifact":{
					"skill_name":"keyword_frequency",
					"summary":{"document_count":2},
					"top_terms":[{"term":"결제","count":2}]
				}
			}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := PythonAIClient{
		BaseURL:      server.URL,
		HTTPClient:   server.Client(),
		ArtifactRoot: artifactRoot,
	}

	result, err := client.Run(context.Background(), domain.ExecutionSummary{
		ExecutionID: "exec-filter",
		ProjectID:   "project-1",
		Plan: domain.SkillPlan{
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "document_filter", DatasetName: "/tmp/issues.prepared.parquet", Inputs: map[string]any{"text_column": "normalized_text", "query": "결제 오류"}},
				{StepID: "step-2", SkillName: "keyword_frequency", DatasetName: "/tmp/issues.prepared.parquet", Inputs: map[string]any{"text_column": "normalized_text", "top_n": 3}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	filterArtifact := result.Artifacts["step:step-1:document_filter"]
	if !strings.Contains(filterArtifact, `"artifact_ref":"`) {
		t.Fatalf("expected artifact_ref in stored document_filter artifact: %s", filterArtifact)
	}
	if strings.Contains(filterArtifact, `"matched_indices"`) {
		t.Fatalf("stored document_filter artifact should be compacted: %s", filterArtifact)
	}
	if !strings.Contains(result.Artifacts["step:step-2:keyword_frequency"], `"document_count":2`) {
		t.Fatalf("unexpected keyword_frequency artifact: %s", result.Artifacts["step:step-2:keyword_frequency"])
	}
}

func TestPythonAIClientStoresDeduplicateDocumentsAsSidecarRefButKeepsRuntimeArtifactForNextStep(t *testing.T) {
	artifactRoot := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
		switch r.URL.Path {
		case "/tasks/deduplicate_documents":
			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("unexpected unmarshal error: %v", err)
			}
			step := payload["step"].(map[string]any)
			inputs := step["inputs"].(map[string]any)
			outputPath := inputs["artifact_output_path"].(string)
			expectedSuffix := filepath.Join("steps", "step-1.deduplicate_documents.rows.parquet")
			if !strings.HasPrefix(outputPath, artifactRoot) || !strings.HasSuffix(outputPath, expectedSuffix) {
				t.Fatalf("unexpected deduplicate output path: %s", outputPath)
			}
			if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
				t.Fatalf("unexpected mkdir error: %v", err)
			}
			if err := os.WriteFile(outputPath, []byte("parquet-placeholder"), 0o644); err != nil {
				t.Fatalf("unexpected write error: %v", err)
			}
			_, _ = w.Write([]byte(`{
				"notes":["dedup completed"],
				"artifact":{
					"skill_name":"deduplicate_documents",
					"summary":{"input_row_count":3,"canonical_row_count":2,"duplicate_row_count":1,"duplicate_group_count":1,"duplicate_threshold":0.8},
					"canonical_indices":[0,2],
					"duplicate_records":[{"source_index":1,"canonical_source_index":0,"similarity":1.0,"text":"결제 오류"}],
					"duplicate_groups":[{"group_id":"duplicate-01","canonical_source_index":0,"duplicate_source_indices":[1],"member_count":2,"samples":["결제 오류","결제 오류!!"]}],
					"artifact_storage_mode":"sidecar_ref",
					"artifact_ref":"` + outputPath + `",
					"artifact_format":"parquet",
					"row_id_column":"row_id",
					"source_index_column":"source_index",
					"canonical_row_id_column":"canonical_row_id",
					"canonical_source_index_column":"canonical_source_index",
					"group_id_column":"group_id",
					"status_column":"dedup_status",
					"similarity_column":"similarity",
					"member_count_column":"member_count"
				}
			}`))
		case "/tasks/keyword_frequency":
			if !strings.Contains(string(body), `"artifact_ref":"`) {
				t.Fatalf("expected compacted deduplicate_documents artifact_ref in prior artifacts, got: %s", string(body))
			}
			if strings.Contains(string(body), `"canonical_indices":[0,2]`) {
				t.Fatalf("expected runtime deduplicate_documents artifact to omit canonical_indices, got: %s", string(body))
			}
			_, _ = w.Write([]byte(`{
				"notes":["keyword completed"],
				"artifact":{
					"skill_name":"keyword_frequency",
					"summary":{"document_count":2},
					"top_terms":[{"term":"결제","count":1},{"term":"로그인","count":1}]
				}
			}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := PythonAIClient{
		BaseURL:      server.URL,
		HTTPClient:   server.Client(),
		ArtifactRoot: artifactRoot,
	}

	result, err := client.Run(context.Background(), domain.ExecutionSummary{
		ExecutionID: "exec-dedup",
		ProjectID:   "project-1",
		Plan: domain.SkillPlan{
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "deduplicate_documents", DatasetName: "/tmp/issues.prepared.parquet", Inputs: map[string]any{"text_column": "normalized_text", "duplicate_threshold": 0.8}},
				{StepID: "step-2", SkillName: "keyword_frequency", DatasetName: "/tmp/issues.prepared.parquet", Inputs: map[string]any{"text_column": "normalized_text", "top_n": 3}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dedupArtifact := result.Artifacts["step:step-1:deduplicate_documents"]
	if !strings.Contains(dedupArtifact, `"artifact_ref":"`) {
		t.Fatalf("expected artifact_ref in stored deduplicate artifact: %s", dedupArtifact)
	}
	if strings.Contains(dedupArtifact, `"canonical_indices"`) {
		t.Fatalf("stored deduplicate artifact should be compacted: %s", dedupArtifact)
	}
	if !strings.Contains(result.Artifacts["step:step-2:keyword_frequency"], `"document_count":2`) {
		t.Fatalf("unexpected keyword_frequency artifact: %s", result.Artifacts["step:step-2:keyword_frequency"])
	}
}

func TestPythonAIClientStoresSentenceSplitAsSidecarRef(t *testing.T) {
	artifactRoot := t.TempDir()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
		switch r.URL.Path {
		case "/tasks/sentence_split":
			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("unexpected unmarshal error: %v", err)
			}
			step := payload["step"].(map[string]any)
			inputs := step["inputs"].(map[string]any)
			outputPath := inputs["artifact_output_path"].(string)
			expectedSuffix := filepath.Join("steps", "step-1.sentence_split.rows.parquet")
			if !strings.HasPrefix(outputPath, artifactRoot) || !strings.HasSuffix(outputPath, expectedSuffix) {
				t.Fatalf("unexpected sentence_split output path: %s", outputPath)
			}
			if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
				t.Fatalf("unexpected mkdir error: %v", err)
			}
			if err := os.WriteFile(outputPath, []byte("parquet-placeholder"), 0o644); err != nil {
				t.Fatalf("unexpected write error: %v", err)
			}
			_, _ = w.Write([]byte(`{
				"notes":["sentence split completed"],
				"artifact":{
					"skill_name":"sentence_split",
					"language":"ko",
					"artifact_storage_mode":"sidecar_ref",
					"artifact_ref":"` + outputPath + `",
					"artifact_format":"parquet",
					"row_id_column":"row_id",
					"source_index_column":"source_index",
					"sentence_index_column":"sentence_index",
					"sentence_text_column":"sentence_text",
					"char_start_column":"char_start",
					"char_end_column":"char_end",
					"summary":{"document_count":2,"sentence_count":4,"splitter_backend":"kss"},
					"sample_documents":[{"source_index":0,"sentence_count":2,"sentences":[{"sentence_index":0,"sentence_text":"결제 오류입니다"}]}]
				}
			}`))
		default:
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client := PythonAIClient{
		BaseURL:      server.URL,
		HTTPClient:   server.Client(),
		ArtifactRoot: artifactRoot,
	}

	result, err := client.Run(context.Background(), domain.ExecutionSummary{
		ExecutionID: "exec-sentences",
		ProjectID:   "project-1",
		Plan: domain.SkillPlan{
			Steps: []domain.SkillPlanStep{
				{StepID: "step-1", SkillName: "sentence_split", DatasetName: "/tmp/issues.prepared.parquet", Inputs: map[string]any{"text_column": "normalized_text"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	sentenceArtifact := result.Artifacts["step:step-1:sentence_split"]
	if !strings.Contains(sentenceArtifact, `"artifact_ref":"`) {
		t.Fatalf("expected artifact_ref in stored sentence_split artifact: %s", sentenceArtifact)
	}
	if !strings.Contains(sentenceArtifact, `"sample_documents"`) {
		t.Fatalf("expected sample_documents preview in stored sentence_split artifact: %s", sentenceArtifact)
	}
}
