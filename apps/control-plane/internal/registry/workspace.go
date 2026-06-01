package registry

import (
	"os"
	"path/filepath"
	"strings"
)

// detectWorkspaceRoot — registry json 파일을 찾을 워크스페이스 root를 추적한다.
// SKILL_BUNDLE_PATH override가 사라진 δ-4 이후 task_registry.json 단일 기준으로 동작한다.
func detectWorkspaceRoot() string {
	cwd, err := os.Getwd()
	if err != nil || strings.TrimSpace(cwd) == "" {
		return "."
	}
	dir := cwd
	for {
		if fileExists(filepath.Join(dir, "config", "task_registry.json")) ||
			fileExists(filepath.Join(dir, "compose.dev.yml")) ||
			fileExists(filepath.Join(dir, "AGENTS.md")) {
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
