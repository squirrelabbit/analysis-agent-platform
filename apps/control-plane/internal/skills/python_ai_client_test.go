package skills

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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
					"summary":{"document_count":2},
					"top_terms":[{"term":"error","count":3}]
				}
			}`))
		case "/tasks/semantic_search":
			if !strings.Contains(string(body), `"query":"관련 문서를 찾아줘"`) {
				t.Fatalf("unexpected semantic search request body: %s", string(body))
			}
			_, _ = w.Write([]byte(`{
				"notes":["semantic search completed"],
				"artifact":{
					"skill_name":"semantic_search",
					"summary":{"match_count":2},
					"matches":[{"rank":1,"score":0.9,"text":"결제 오류"}]
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
					"summary":"대표 이슈 근거를 모았습니다",
					"evidence":[{"rank":1,"source_index":0,"snippet":"결제 오류","rationale":"selected"}]
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
					"summary":"대표 이슈 근거를 모았습니다",
					"evidence":[{"rank":1,"source_index":0,"snippet":"결제 오류","rationale":"selected"}]
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

	result, err := client.Run(context.Background(), domain.ExecutionSummary{
		ExecutionID: "exec-1",
		ProjectID:   "project-1",
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
					DatasetName: "/tmp/issues.sentiment.jsonl",
					Inputs: map[string]any{
						"text_column":      "normalized_text",
						"sentiment_column": "sentiment_label",
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
	evidenceArtifact := result.Artifacts["step:step-5:issue_evidence_summary"]
	if !strings.Contains(evidenceArtifact, `"selection_source":"semantic_search"`) {
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
				"artifact":{"skill_name":"document_filter","matched_indices":[0,2],"summary":{"filtered_row_count":2}}
			}`))
		case "/tasks/keyword_frequency":
			_, _ = w.Write([]byte(`{
				"notes":["keyword path completed"],
				"artifact":{"skill_name":"keyword_frequency","summary":{"document_count":2},"top_terms":[{"term":"결제","count":2}]}
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
				{StepID: "step-3", SkillName: "time_bucket_count", DatasetName: "/tmp/issues.csv", Inputs: map[string]any{"text_column": "text", "time_column": "occurred_at", "bucket": "day"}},
				{StepID: "step-4", SkillName: "meta_group_count", DatasetName: "/tmp/issues.csv", Inputs: map[string]any{"text_column": "text", "dimension_column": "channel"}},
				{StepID: "step-5", SkillName: "document_sample", DatasetName: "/tmp/issues.csv", Inputs: map[string]any{"text_column": "text", "query": "결제 오류"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ProcessedSteps != 5 {
		t.Fatalf("unexpected processed steps: %d", result.ProcessedSteps)
	}
	if !strings.Contains(result.Artifacts["step:step-2:keyword_frequency"], `"결제"`) {
		t.Fatalf("unexpected keyword artifact: %s", result.Artifacts["step:step-2:keyword_frequency"])
	}
	if !strings.Contains(result.Artifacts["step:step-5:document_sample"], `"sample_count":2`) {
		t.Fatalf("unexpected sample artifact: %s", result.Artifacts["step:step-5:document_sample"])
	}
}
