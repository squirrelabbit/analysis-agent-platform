package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/workererror"
)

// silverone 2026-06-29 (ADR-031 4단계) — runWorkerTask / workerTaskResponse /
// workerTaskTimeout 제거. dataset_build worker 호출은 skills.PythonBuildClient의
// RunTask / RunDatasetClean(buildTaskTimeout)로 통일됐다. 두 경로가 완전 중복이었고
// timeout 값(clean 20분 / clause·doc 90분 / default 2분)과 response shape도 동일해
// 동작 불변. (binding timeout이 workerTaskTimeout → buildTaskTimeout으로 바뀌었으나
// 값이 같아 90분 정렬은 유지된다.)

// cancelWorkerBuild — 실행 중인 build task에 협조적 취소 신호를 보낸다(silverone 2026-06-29).
// worker `/tasks/cancel`에 dataset_version_id를 보내면 그 version의 실행 중 task가 event를
// 확인해 남은 doc 처리를 멈추고 거기까지 결과를 flush 후 정상 반환한다. best-effort —
// 짧은 timeout, worker가 못 받아도(이미 끝남 등) 에러로 취급하지 않는다(found=false).
func (s *DatasetService) cancelWorkerBuild(ctx context.Context, datasetVersionID string) error {
	baseURL := strings.TrimRight(strings.TrimSpace(s.pythonAIWorkerURL), "/")
	if baseURL == "" {
		return errors.New("python ai worker url is required")
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	body, err := json.Marshal(map[string]any{"dataset_version_id": datasetVersionID})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(timeoutCtx, http.MethodPost, baseURL+"/tasks/cancel", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	_, _ = workererror.ReadAllAndClose(resp.Body)
	if resp.StatusCode >= 500 {
		return errors.New("worker cancel failed")
	}
	return nil
}
