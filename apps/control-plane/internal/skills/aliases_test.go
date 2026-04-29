package skills

import "testing"

func TestCanonicalSkillName(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
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
	if !IsAliasFor("term_frequency", "term_frequency") {
		t.Fatal("expected identical skill names to match")
	}
	if IsAliasFor("noun_frequency", "term_frequency") {
		t.Fatal("expected unrelated skills not to match")
	}
}
