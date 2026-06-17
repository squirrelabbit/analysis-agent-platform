package skills

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/registry"
	"analysis-support-platform/control-plane/internal/workererror"
)

// PythonBuildClient는 dataset_build 단계 (clean / clause_label /
// doc_genuineness)의 Python AI worker HTTP 호출을 책임진다. 옛 prepare /
// sentiment_label / embedding / cluster / segment / embedding_cluster /
// keyword_index 7 task + document_cluster_profile 1 task는 (β2 / 5/19) 결정으로
// 제거되었다.
//
// 5/11 결정 (silverone) — dataset_build를 plan skill과 분리. concept적으로
// dataset_build는 *plan에 들어가지 않는 infra-level orchestration*이므로 plan
// execution과 독립된 client로 둔다. δ-4 (5/21)로 `skill_bundle.json` 자체가
// 삭제됐고, 남은 dataset_build task 카탈로그는 `config/task_registry.json`.
//
// 기존 `service.DatasetService.runWorkerTask` 로직을 그대로 이동했다 — taskPath
// 별 timeout, 4xx → NonRetryableApplicationError wrap, 5xx → Upstream error.
//
// taskPath 별 명시 method(`RunDatasetClean` 등)는 *thin wrapper*로 제공해
// 호출처(`dataset_build_*.go`)가 generic `RunTask` 대신 의도 명확한 method
// 호출 가능. 신규 build 단계 추가 시 *method 1개 + timeout 1줄*만 추가하면
// 된다.
type PythonBuildClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

// PythonBuildTaskResponse는 dataset_build task의 표준 응답 shape.
//
// Artifact는 단일 artifact를 반환하는 기존 build skill용. 5/7 결정 5-step
// pipeline의 dataset_embedding_cluster / dataset_keyword_index는 1 step = 2
// artifact를 반환하므로 Artifacts list를 우선 사용한다. 단일/list 둘 다
// 지원해서 호출자가 호환성 유지.
type PythonBuildTaskResponse struct {
	Notes     []string         `json:"notes"`
	Artifact  map[string]any   `json:"artifact"`
	Artifacts []map[string]any `json:"artifacts,omitempty"`
}

// AllArtifacts는 worker response를 단일/multi artifact 양쪽 모두에서 추출.
// list가 있으면 list 사용, 없으면 단일 artifact를 list로 wrap.
func (r PythonBuildTaskResponse) AllArtifacts() []map[string]any {
	if len(r.Artifacts) > 0 {
		return r.Artifacts
	}
	if r.Artifact != nil {
		return []map[string]any{r.Artifact}
	}
	return nil
}

// NewPythonBuildClient는 PythonBuildClient를 생성한다. httpClient가 nil이면
// 기본 http.Client를 만든다 — DatasetService는 자기 httpClient를 공유해서
// 주입하므로 nil이 아닐 것.
func NewPythonBuildClient(baseURL string, httpClient *http.Client) *PythonBuildClient {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &PythonBuildClient{
		BaseURL:    strings.TrimSpace(baseURL),
		HTTPClient: httpClient,
	}
}

// RunTask는 generic dataset_build task 호출. taskPath는 `/tasks/<name>` 형태.
// 명시 method (`RunDatasetClean` 등)가 내부적으로 호출한다.
//
// 4xx (worker validation 거부)는 NonRetryableApplicationError로 wrap한다 —
// dataset build 단계가 Temporal activity 안에서 호출되므로 retry storm 방지.
// 5xx는 Upstream error로 wrap해서 retry policy 그대로 적용.
func (c *PythonBuildClient) RunTask(ctx context.Context, taskPath string, payload map[string]any) (PythonBuildTaskResponse, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(c.BaseURL), "/")
	if baseURL == "" {
		return PythonBuildTaskResponse{}, errors.New("python ai worker url is required")
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, buildTaskTimeout(taskPath))
	defer cancel()

	body, err := json.Marshal(payload)
	if err != nil {
		return PythonBuildTaskResponse{}, err
	}
	req, err := http.NewRequestWithContext(timeoutCtx, http.MethodPost, baseURL+taskPath, bytes.NewReader(body))
	if err != nil {
		return PythonBuildTaskResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return PythonBuildTaskResponse{}, err
	}
	bodyBytes, readErr := workererror.ReadAllAndClose(resp.Body)
	if readErr != nil {
		return PythonBuildTaskResponse{}, readErr
	}

	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		return PythonBuildTaskResponse{}, workererror.Rejection(resp.StatusCode, bodyBytes)
	}
	if resp.StatusCode >= 500 {
		return PythonBuildTaskResponse{}, workererror.Upstream(resp.StatusCode, bodyBytes)
	}

	var decoded PythonBuildTaskResponse
	if err := json.Unmarshal(bodyBytes, &decoded); err != nil {
		return PythonBuildTaskResponse{}, err
	}
	return decoded, nil
}

// taskPath 별 timeout. 신규 build 단계 추가 시 case 1개 추가.
//
// 주의: doc_genuineness / clause_label은 datasetBuildExecuteActivityOptions의
// StartToCloseTimeout(90분)와 맞춰야 한다. HTTP 타임아웃이 더 짧으면(과거 30분)
// 액티비티가 90분을 허용해도 HTTP 호출이 먼저 끊겨 "worker_timeout"으로 빌드가
// 실패한다 — 특히 교차검증(verify, 2모델+judge)은 2k doc 기준 25~50분 소요라
// 30분 한도를 넘겼다 (2026-06-17). 두 값을 90분으로 정렬한다.
func buildTaskTimeout(taskPath string) time.Duration {
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

// RunDatasetClean은 clean 단계의 명시 wrapper. Python worker
// `dataset_build.py::run_dataset_clean`을 호출하며, 내부에서 noise_patterns
// (tier 4 config fallback: `config/noise_patterns/festival-v1.json`)을 자동
// load해 inline scrub을 수행한다. silverone 검증(2026-05-11) 결과 vault
// `검토-raw/document_cluster_검증_2026-05-11.md` §3에 기록.
func (c *PythonBuildClient) RunDatasetClean(ctx context.Context, payload map[string]any) (PythonBuildTaskResponse, error) {
	return c.RunTask(ctx, registry.TaskPathFor("dataset_clean"), payload)
}

