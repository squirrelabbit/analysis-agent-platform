package service

import (
	"context"

	"analysis-support-platform/control-plane/internal/skills"
)

// datasetBuildTaskClient — DatasetService가 Python worker의 dataset_build task를 호출하는
// 경계(port, ADR-031 4단계). concrete `*skills.PythonBuildClient`가 이 인터페이스를
// satisfy하고, 테스트는 fake를 `buildClientOverride`로 주입해 실제 worker 없이 build
// orchestration을 검증할 수 있다.
//
// 모든 dataset_build worker 호출은 이 port의 RunTask/RunDatasetClean으로 통일됐다
// (ADR-031 4단계, 2026-06-29 — 옛 runWorkerTask 2경로 제거). 호출부는 인터페이스에
// 의존하므로 테스트에서 fake(buildClientOverride)를 주입해 worker 없이 검증할 수 있다.
type datasetBuildTaskClient interface {
	RunTask(ctx context.Context, taskPath string, payload map[string]any) (skills.PythonBuildTaskResponse, error)
	RunDatasetClean(ctx context.Context, payload map[string]any) (skills.PythonBuildTaskResponse, error)
}
