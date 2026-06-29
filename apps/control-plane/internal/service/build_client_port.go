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
// 배경: 현재 worker 호출은 `runWorkerTask`(DatasetService 메서드)와
// `PythonBuildClient.RunTask`로 2경로 혼재한다(struct 주석 참조). 이 port가 그 통일의
// 첫 단계 — 호출부는 그대로 두고 의존을 인터페이스로 바꿔 fake 가능성부터 확보한다.
type datasetBuildTaskClient interface {
	RunTask(ctx context.Context, taskPath string, payload map[string]any) (skills.PythonBuildTaskResponse, error)
	RunDatasetClean(ctx context.Context, payload map[string]any) (skills.PythonBuildTaskResponse, error)
}
