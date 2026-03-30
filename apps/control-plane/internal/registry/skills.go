package registry

var SupportedSkills = []string{
	"structured_kpi_summary",
	"document_filter",
	"keyword_frequency",
	"time_bucket_count",
	"meta_group_count",
	"document_sample",
	"unstructured_issue_summary",
	"issue_breakdown_summary",
	"issue_trend_summary",
	"issue_period_compare",
	"issue_sentiment_summary",
	"semantic_search",
	"issue_evidence_summary",
	"evidence_pack",
}

var MVPDefaultSkills = []string{
	"structured_kpi_summary",
	"unstructured_issue_summary",
	"issue_evidence_summary",
}

var MVPSkillsByDataType = map[string][]string{
	"structured":   {"structured_kpi_summary"},
	"unstructured": {"unstructured_issue_summary", "issue_evidence_summary"},
	"mixed":        {"structured_kpi_summary", "unstructured_issue_summary", "semantic_search", "issue_evidence_summary"},
	"both":         {"structured_kpi_summary", "unstructured_issue_summary", "semantic_search", "issue_evidence_summary"},
}
