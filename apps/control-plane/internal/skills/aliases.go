package skills

import "strings"

// DeprecatedAliases maps deprecated skill names to canonical replacements.
var DeprecatedAliases = map[string]string{}

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
