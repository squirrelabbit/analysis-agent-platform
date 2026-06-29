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


type workerTaskResponse struct {
	Notes []string       `json:"notes"`
	// Artifact는 단일 artifact를 반환하는 기존 build skill용. 5/7 결정 5-step
	// pipeline의 dataset_embedding_cluster / dataset_keyword_index는 1 step =
	// 2 artifact를 반환하므로 Artifacts list를 우선 사용한다. 단일/list 둘 다
	// 지원해서 호출자가 호환성 유지.
	Artifact  map[string]any   `json:"artifact"`
	Artifacts []map[string]any `json:"artifacts,omitempty"`
}

func (s *DatasetService) runWorkerTask(ctx context.Context, taskPath string, payload map[string]any) (workerTaskResponse, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(s.pythonAIWorkerURL), "/")
	if baseURL == "" {
		return workerTaskResponse{}, errors.New("python ai worker url is required")
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, workerTaskTimeout(taskPath))
	defer cancel()

	body, err := json.Marshal(payload)
	if err != nil {
		return workerTaskResponse{}, err
	}
	req, err := http.NewRequestWithContext(timeoutCtx, http.MethodPost, baseURL+taskPath, bytes.NewReader(body))
	if err != nil {
		return workerTaskResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return workerTaskResponse{}, err
	}
	bodyBytes, readErr := workererror.ReadAllAndClose(resp.Body)
	if readErr != nil {
		return workerTaskResponse{}, readErr
	}

	// 5/6 fix: dataset build 단계가 Temporal activity 안에서 호출되므로
	// 4xx (worker validation 거부)는 NonRetryableApplicationError로 wrap해야
	// retry storm을 막는다. 5xx는 일반 error로 retry policy 그대로.
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return workerTaskResponse{}, workererror.Rejection(resp.StatusCode, bodyBytes)
	}
	if resp.StatusCode >= 500 {
		return workerTaskResponse{}, workererror.Upstream(resp.StatusCode, bodyBytes)
	}

	var decoded workerTaskResponse
	if err := json.Unmarshal(bodyBytes, &decoded); err != nil {
		return workerTaskResponse{}, err
	}
	return decoded, nil
}

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

// workerTaskTimeout — runWorkerTask(서비스 경로)가 Python worker HTTP 호출에 거는
// 컨텍스트 타임아웃. dataset build(clause_label/doc_genuineness)는 이 경로가 Temporal
// 활동 안에서 호출되므로 활동 StartToCloseTimeout(datasetBuildExecuteActivityOptions,
// 90분) 및 python_build_client.buildTaskTimeout(90분)과 반드시 정렬해야 한다.
//
// silverone 2026-06-18 — verify(2모델+judge)+chunking 빌드가 30.8분 걸려 30분 한도를
// 1분 차로 넘겨 worker_timeout으로 실패(데이터는 정상 기록)한 사례 확인. faba3996이
// buildTaskTimeout만 90분으로 올렸으나 build 경로는 이 함수(runWorkerTask)를 쓰므로
// 30분이 그대로 binding이었다. 두 LLOA 단계를 90분으로 정렬한다.
func workerTaskTimeout(taskPath string) time.Duration {
	switch strings.TrimSpace(taskPath) {
	case "/tasks/dataset_clean":
		return 20 * time.Minute
	case "/tasks/dataset_clause_label":
		return 90 * time.Minute
	case "/tasks/dataset_doc_genuineness":
		return 90 * time.Minute
	default:
		return 2 * time.Minute
	}
}
