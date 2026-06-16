package registry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// TaskRegistry — ADR-017로 도입된 *control plane 내부 실행 task* 카탈로그.
// skill_bundle.json은 planner skill 전용으로 정화하고, dataset_build / admin /
// evaluation 같은 내부 task routing 메타데이터는 task_registry.json에서 관리.
type TaskRegistry struct {
	Version     string           `json:"version"`
	Tasks       []TaskDefinition `json:"tasks"`
	tasksByName map[string]TaskDefinition
}

type TaskDefinition struct {
	TaskName        string `json:"task_name"`
	Engine          string `json:"engine"`
	TaskPath        string `json:"task_path"`
	Kind            string `json:"kind"`
	Visibility      string `json:"visibility"`
	Description     string `json:"description"`
	DatasetSource   string `json:"dataset_source"`
	RequiresPrepare bool   `json:"requires_prepare"`
	RequiresClean   bool   `json:"requires_clean"`
	ResultKind      string `json:"result_kind"`
	ResultScope     string `json:"result_scope"`
	FallbackPolicy  string `json:"fallback_policy"`
	QualityTier     string `json:"quality_tier"`
}

var (
	taskLoadOnce   sync.Once
	cachedRegistry TaskRegistry
	taskLoadErr    error
)

func taskRegistry() TaskRegistry {
	taskLoadOnce.Do(func() {
		cachedRegistry, taskLoadErr = loadTaskRegistry()
	})
	if taskLoadErr != nil {
		panic(taskLoadErr)
	}
	return cachedRegistry
}

// Task — task_name으로 TaskDefinition lookup. control plane이 실행 가능한
// task인지 판정할 때 사용.
func Task(name string) (TaskDefinition, bool) {
	definition, ok := taskRegistry().tasksByName[strings.TrimSpace(name)]
	return definition, ok
}

// TaskPathFor — task_name → task_path lookup. service 코드가 runWorkerTask
// 호출 시 hardcoded "/tasks/<name>" 대신 이걸 쓴다. 등록 안 된 task는 ""
// 반환 (호출처에서 명시적 error 처리). panic은 control plane 전체를
// 죽이므로 피한다 (ADR-017 §7 Open risk 2).
func TaskPathFor(name string) string {
	definition, ok := Task(name)
	if !ok {
		return ""
	}
	return definition.TaskPath
}

func loadTaskRegistry() (TaskRegistry, error) {
	path := resolveTaskRegistryPath()
	content, err := os.ReadFile(path)
	if err != nil {
		return TaskRegistry{}, fmt.Errorf("read task registry %s: %w", path, err)
	}

	var parsed TaskRegistry
	if err := json.Unmarshal(content, &parsed); err != nil {
		return TaskRegistry{}, fmt.Errorf("decode task registry %s: %w", path, err)
	}

	parsed.tasksByName = make(map[string]TaskDefinition, len(parsed.Tasks))
	for index := range parsed.Tasks {
		parsed.Tasks[index].TaskName = strings.TrimSpace(parsed.Tasks[index].TaskName)
		if parsed.Tasks[index].TaskName == "" {
			return TaskRegistry{}, fmt.Errorf("task registry %s contains empty task_name", path)
		}
		if _, exists := parsed.tasksByName[parsed.Tasks[index].TaskName]; exists {
			return TaskRegistry{}, fmt.Errorf("task registry %s contains duplicate task_name %s", path, parsed.Tasks[index].TaskName)
		}
		parsed.tasksByName[parsed.Tasks[index].TaskName] = parsed.Tasks[index]
	}

	if strings.TrimSpace(parsed.Version) == "" {
		return TaskRegistry{}, fmt.Errorf("task registry %s is missing version", path)
	}
	return parsed, nil
}

func resolveTaskRegistryPath() string {
	override := strings.TrimSpace(os.Getenv("TASK_REGISTRY_PATH"))
	root := detectWorkspaceRoot()
	if override == "" {
		return filepath.Join(root, "config", "task_registry.json")
	}
	if filepath.IsAbs(override) {
		return override
	}
	return filepath.Join(root, override)
}
