package registry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type SkillBundle struct {
	Version          string              `json:"version"`
	DefaultPlans     map[string][]string `json:"default_plans"`
	PlannerSequences map[string][]string `json:"planner_sequences"`
	Skills           []SkillDefinition   `json:"skills"`
	skillsByName     map[string]SkillDefinition
}

type SkillDefinition struct {
	Name              string            `json:"name"`
	Description       string            `json:"description"`
	Engine            string            `json:"engine"`
	Kind              string            `json:"kind"`
	Category          string            `json:"category"`
	PlanEnabled       bool              `json:"plan_enabled"`
	TaskPath          string            `json:"task_path"`
	DatasetSource     string            `json:"dataset_source"`
	RequiresPrepare   bool              `json:"requires_prepare"`
	RequiresSentiment bool              `json:"requires_sentiment"`
	RequiresEmbedding bool              `json:"requires_embedding"`
	GoalInput         string            `json:"goal_input"`
	DefaultInputs     map[string]any    `json:"default_inputs"`
	MetadataDefaults  map[string]string `json:"metadata_defaults"`
}

var (
	loadOnce     sync.Once
	cachedBundle SkillBundle
	loadErr      error
)

func bundle() SkillBundle {
	loadOnce.Do(func() {
		cachedBundle, loadErr = loadBundle()
	})
	if loadErr != nil {
		panic(loadErr)
	}
	return cachedBundle
}

func BundleVersion() string {
	return bundle().Version
}

func Skill(name string) (SkillDefinition, bool) {
	definition, ok := bundle().skillsByName[strings.TrimSpace(name)]
	return definition, ok
}

func SupportedSkills() []string {
	skills := bundle().Skills
	names := make([]string, 0, len(skills))
	for _, skill := range skills {
		if skill.PlanEnabled {
			names = append(names, skill.Name)
		}
	}
	return names
}

func DefaultPlanSkills(dataType string) []string {
	selected := bundle().DefaultPlans[strings.TrimSpace(dataType)]
	if len(selected) == 0 {
		selected = bundle().DefaultPlans["structured"]
	}
	return append([]string(nil), selected...)
}

func PlannerSequence(name string) []string {
	selected := bundle().PlannerSequences[strings.TrimSpace(name)]
	return append([]string(nil), selected...)
}

func PlanSkillNames() []string {
	return SupportedSkills()
}

func loadBundle() (SkillBundle, error) {
	path := resolveBundlePath()
	content, err := os.ReadFile(path)
	if err != nil {
		return SkillBundle{}, fmt.Errorf("read skill bundle %s: %w", path, err)
	}

	var parsed SkillBundle
	if err := json.Unmarshal(content, &parsed); err != nil {
		return SkillBundle{}, fmt.Errorf("decode skill bundle %s: %w", path, err)
	}

	parsed.skillsByName = make(map[string]SkillDefinition, len(parsed.Skills))
	for index := range parsed.Skills {
		parsed.Skills[index].Name = strings.TrimSpace(parsed.Skills[index].Name)
		if parsed.Skills[index].DefaultInputs == nil {
			parsed.Skills[index].DefaultInputs = map[string]any{}
		}
		if parsed.Skills[index].MetadataDefaults == nil {
			parsed.Skills[index].MetadataDefaults = map[string]string{}
		}
		if parsed.Skills[index].Name == "" {
			return SkillBundle{}, fmt.Errorf("skill bundle %s contains empty skill name", path)
		}
		if _, exists := parsed.skillsByName[parsed.Skills[index].Name]; exists {
			return SkillBundle{}, fmt.Errorf("skill bundle %s contains duplicate skill %s", path, parsed.Skills[index].Name)
		}
		parsed.skillsByName[parsed.Skills[index].Name] = parsed.Skills[index]
	}

	if strings.TrimSpace(parsed.Version) == "" {
		return SkillBundle{}, fmt.Errorf("skill bundle %s is missing version", path)
	}
	if parsed.DefaultPlans == nil {
		parsed.DefaultPlans = map[string][]string{}
	}
	if parsed.PlannerSequences == nil {
		parsed.PlannerSequences = map[string][]string{}
	}
	return parsed, nil
}

func resolveBundlePath() string {
	override := strings.TrimSpace(os.Getenv("SKILL_BUNDLE_PATH"))
	root := detectWorkspaceRoot()
	if override == "" {
		return filepath.Join(root, "config", "skill_bundle.json")
	}
	if filepath.IsAbs(override) {
		return override
	}
	return filepath.Join(root, override)
}

func detectWorkspaceRoot() string {
	cwd, err := os.Getwd()
	if err != nil || strings.TrimSpace(cwd) == "" {
		return "."
	}
	dir := cwd
	for {
		if fileExists(filepath.Join(dir, "config", "skill_bundle.json")) || fileExists(filepath.Join(dir, "compose.dev.yml")) || fileExists(filepath.Join(dir, "AGENTS.md")) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return cwd
		}
		dir = parent
	}
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}
