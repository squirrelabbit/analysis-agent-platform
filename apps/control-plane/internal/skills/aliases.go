package skills

import "strings"

// DeprecatedAliases mirrors ADR-009 F1 alias handling so Go control-plane can
// treat deprecated skill names the same way as the Python runtime.
var DeprecatedAliases = map[string]string{
	"keyword_frequency": "term_frequency",
}

// CanonicalSkillName resolves a deprecated skill name to its canonical name.
func CanonicalSkillName(name string) string {
	trimmed := strings.TrimSpace(name)
	if canonical, ok := DeprecatedAliases[trimmed]; ok {
		return canonical
	}
	return trimmed
}

// IsAliasFor reports whether both names resolve to the same canonical skill.
func IsAliasFor(left, right string) bool {
	return CanonicalSkillName(left) == CanonicalSkillName(right)
}
