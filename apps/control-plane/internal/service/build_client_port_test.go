package service

import (
	"context"
	"testing"

	"analysis-support-platform/control-plane/internal/skills"
)

// fakeBuildTaskClient — datasetBuildTaskClient port의 테스트 fake.
type fakeBuildTaskClient struct {
	runTaskPaths  []string
	runCleanCount int
}

func (f *fakeBuildTaskClient) RunTask(_ context.Context, taskPath string, _ map[string]any) (skills.PythonBuildTaskResponse, error) {
	f.runTaskPaths = append(f.runTaskPaths, taskPath)
	return skills.PythonBuildTaskResponse{}, nil
}

func (f *fakeBuildTaskClient) RunDatasetClean(_ context.Context, _ map[string]any) (skills.PythonBuildTaskResponse, error) {
	f.runCleanCount++
	return skills.PythonBuildTaskResponse{}, nil
}

// TestBuildClientUsesOverride — ADR-031 4단계: buildClientOverride가 주입되면
// concrete 대신 그 fake를 쓴다(worker 없이 build orchestration 테스트 가능).
func TestBuildClientUsesOverride(t *testing.T) {
	fake := &fakeBuildTaskClient{}
	svc := &DatasetService{buildClientOverride: fake}
	if got := svc.buildClient(); got != fake {
		t.Fatalf("buildClient()이 주입된 override fake를 반환하지 않음")
	}
}

// TestBuildClientFallsBackToConcrete — override가 없으면 concrete PythonBuildClient를
// lazy 생성해 반환한다(운영 무영향).
func TestBuildClientFallsBackToConcrete(t *testing.T) {
	svc := &DatasetService{pythonAIWorkerURL: "http://worker.example"}
	if svc.buildClient() == nil {
		t.Fatalf("override 없을 때 concrete PythonBuildClient를 반환해야 함")
	}
}
