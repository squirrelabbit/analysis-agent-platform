package skills

import "testing"

func TestCanonicalSkillName(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "deprecated keyword", raw: "keyword_frequency", want: "term_frequency"},
		{name: "deprecated evidence", raw: "evidence_pack", want: "issue_evidence_summary"},
		{name: "canonical term", raw: "term_frequency", want: "term_frequency"},
		{name: "canonical evidence", raw: "issue_evidence_summary", want: "issue_evidence_summary"},
		{name: "other registered skill", raw: "noun_frequency", want: "noun_frequency"},
		{name: "unknown skill", raw: "unknown", want: "unknown"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := CanonicalSkillName(tc.raw); got != tc.want {
				t.Fatalf("CanonicalSkillName(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestIsAliasFor(t *testing.T) {
	if !IsAliasFor("keyword_frequency", "term_frequency") {
		t.Fatal("expected deprecated and canonical names to match")
	}
	if !IsAliasFor("term_frequency", "keyword_frequency") {
		t.Fatal("expected alias comparison to be symmetric")
	}
	if !IsAliasFor("evidence_pack", "issue_evidence_summary") {
		t.Fatal("expected evidence alias and canonical names to match")
	}
	if !IsAliasFor("issue_evidence_summary", "evidence_pack") {
		t.Fatal("expected evidence alias comparison to be symmetric")
	}
	if IsAliasFor("noun_frequency", "keyword_frequency") {
		t.Fatal("expected unrelated skills not to match")
	}
}
