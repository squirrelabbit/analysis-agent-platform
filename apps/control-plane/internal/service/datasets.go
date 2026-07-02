package service

import (
	"net/http"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/service/datasetprompts"
	"analysis-support-platform/control-plane/internal/skills"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"
)

const DefaultEmbeddingModel = "intfloat/multilingual-e5-small"

const defaultDatasetSourceSummarySampleLimit = 5

const (
	datasetLLMModeDefault  = "default"
	datasetLLMModeEnabled  = "enabled"
	datasetLLMModeDisabled = "disabled"
)

type DatasetService struct {
	store               store.Repository
	pythonAIWorkerURL   string
	uploadRoot          string
	artifactRoot        string
	datasetProfilesPath string
	profileRegistry     *datasetProfileRegistry
	buildJobStarter     workflows.Starter
	httpClient          *http.Client
	// silverone 2026-06-04 — analyze_v2 worker 호출 timeout. config의
	// PYTHON_AI_WORKER_HTTP_TIMEOUT_SEC를 SetPythonAITaskTimeout으로 주입한다.
	// 미설정(0 이하)이면 postPythonAITask가 defaultPythonAITaskTimeout으로 fallback.
	pythonAITaskTimeout time.Duration
	// dataset_build worker 호출은 PythonBuildClient(RunTask/RunDatasetClean)로 통일됨.
	// (5/11 분리 시작 → ADR-031 4단계 2026-06-29 통일 완료 — runWorkerTask 제거.)
	pythonBuildClient *skills.PythonBuildClient
	// silverone 2026-06-29 (ADR-031 4단계) — worker 호출 경계 port. test fake 주입용
	// override. nil이면 concrete pythonBuildClient를 쓴다(운영 무영향). worker 없이
	// build orchestration을 테스트할 수 있게 하는 첫 단계.
	buildClientOverride datasetBuildTaskClient
	// silverone 2026-05-28 subpackage pilot — 옛 dataset_prompts.go (823 lines)을
	// internal/service/datasetprompts/로 분리. DatasetService는 facade method로
	// 같은 public 시그니처를 유지하고 위임만 한다.
	prompts *datasetprompts.Service
	// silverone 2026-06-08 — artifact view 응답에 화면 표시용 모델명을 빌드 재실행
	// 없이 입히기 위한 env 값. lloaModel(현재 raw model)과 artifact summary.model이
	// 같을 때만 lloaModelDisplayName을 노출한다. SetLLOAModelDisplay로 주입.
	lloaModel            string
	lloaModelDisplayName string
	// silverone 2026-06-12 — 전처리 빌드 모델 선택 allowlist (LLOA_MODELS env).
	// SetLLOAModelOptions로 주입. 빈 목록이면 model_id 선택 자체를 거부한다
	// (default env 모델만 사용).
	lloaModelOptions []domain.LLOAModelOption
	// silverone 2026-06-08 — plan reuse(POC-1) 토글. 기본 false(비활성).
	// ANALYSIS_PLAN_REUSE_ENABLED로 SetPlanReuseEnabled를 통해 주입. context
	// hijack(이전 결과 오재표시) 때문에 기본 OFF. threads()가 sub-service로 전달.
	planReuseEnabled bool
}

func NewDatasetService(repository store.Repository, pythonAIWorkerURL string, uploadRoot string, artifactRoot string) *DatasetService {
	return &DatasetService{
		store:             repository,
		pythonAIWorkerURL: pythonAIWorkerURL,
		uploadRoot:        strings.TrimSpace(uploadRoot),
		artifactRoot:      strings.TrimSpace(artifactRoot),
		httpClient:        &http.Client{},
		prompts:           datasetprompts.New(repository),
	}
}

// buildClient는 PythonBuildClient를 lazy 생성/갱신한다. test가
// `service.pythonAIWorkerURL = server.URL`로 직접 필드 할당하는 기존 패턴을
// 깨지 않기 위해 호출 시점에 BaseURL 동기화. 5/11 (silverone) dataset_build를
// plan skill과 분리한다는 결정에 따른 첫 마이그레이션 (clean) 도입.
func (s *DatasetService) buildClient() datasetBuildTaskClient {
	if s.buildClientOverride != nil {
		return s.buildClientOverride
	}
	url := strings.TrimSpace(s.pythonAIWorkerURL)
	if s.pythonBuildClient == nil {
		s.pythonBuildClient = skills.NewPythonBuildClient(url, s.httpClient)
		return s.pythonBuildClient
	}
	if s.pythonBuildClient.BaseURL != url {
		s.pythonBuildClient.BaseURL = url
	}
	return s.pythonBuildClient
}

func (s *DatasetService) SetDatasetProfilesPath(path string) error {
	registry, err := loadDatasetProfileRegistry(path)
	if err != nil {
		return err
	}
	s.datasetProfilesPath = strings.TrimSpace(path)
	s.profileRegistry = registry
	return nil
}

func (s *DatasetService) SetBuildJobStarter(starter workflows.Starter) {
	s.buildJobStarter = starter
}

// SetLLOAModelDisplay — artifact view 응답의 model_display_name 계산용 env 주입.
// model은 현재 LLOA_MODEL(raw id), displayName은 LLOA_MODEL_DISPLAY_NAME.
// 빌드 시점 snapshot이 아니라 응답 시점에 입히므로 .env 변경 후 control-plane
// 재시작만으로 반영된다(전처리 재실행 불필요).
func (s *DatasetService) SetLLOAModelDisplay(model, displayName string) {
	s.lloaModel = strings.TrimSpace(model)
	s.lloaModelDisplayName = strings.TrimSpace(displayName)
}

// SetLLOAModelsPath — 전처리 빌드 모델 선택 allowlist를 config 파일에서 로드해
// 주입한다 (config/lloa_models.json). dataset_profiles.json과 같은 패턴 —
// 파일 부재는 정상(빈 목록), 손상된 JSON은 error로 부팅 시 fail-loud.
func (s *DatasetService) SetLLOAModelsPath(path string) error {
	options, err := loadLLOAModelOptions(path)
	if err != nil {
		return err
	}
	s.lloaModelOptions = options
	return nil
}

// SetLLOAModelOptions — allowlist를 직접 주입 (테스트/직접 배선용). 운영 배선은
// SetLLOAModelsPath가 config 파일에서 로드해 호출한다. 응답 시점 표시명 lookup과
// job 생성 시 model_id 검증에 쓰인다.
func (s *DatasetService) SetLLOAModelOptions(options []domain.LLOAModelOption) {
	s.lloaModelOptions = options
}

// LLOAModelOptions — GET /lloa_model_options 응답용 allowlist 조회.
func (s *DatasetService) LLOAModelOptions() []domain.LLOAModelOption {
	return s.lloaModelOptions
}

// validateLLOAModelID — 빌드 요청의 model_id 검증. nil/빈 값은 default 모델
// 사용으로 통과. allowlist 밖이면 ErrInvalidArgument (400).
func (s *DatasetService) validateLLOAModelID(modelID *string) error {
	if modelID == nil || strings.TrimSpace(*modelID) == "" {
		return nil
	}
	requested := strings.TrimSpace(*modelID)
	allowed := make([]string, 0, len(s.lloaModelOptions))
	for _, opt := range s.lloaModelOptions {
		if opt.ModelID == requested {
			return nil
		}
		allowed = append(allowed, opt.ModelID)
	}
	return ErrInvalidArgument{Message: "model_id not allowed: " + requested + " (allowed: " + strings.Join(allowed, ", ") + ")"}
}

// validateLLOAModelInAllowlist — 단일 모델 id가 allowlist에 있는지(빈 값 거부).
// validateLLOAModelID는 nil/빈 값을 통과시키지만, verify는 모델이 필수라 별도.
func (s *DatasetService) validateLLOAModelInAllowlist(model string) error {
	model = strings.TrimSpace(model)
	if model == "" {
		return ErrInvalidArgument{Message: "model id required"}
	}
	for _, opt := range s.lloaModelOptions {
		if opt.ModelID == model {
			return nil
		}
	}
	return ErrInvalidArgument{Message: "model_id not allowed: " + model}
}

// validateVerifyModels — verify 모드 입력 검증 (ADR-026). classify_models는
// allowlist의 서로 다른 2개, judge_model(지정 시)도 allowlist. allowlist가 비어
// 있으면(LLOA_MODELS 미설정) verify 자체 불가.
func (s *DatasetService) validateVerifyModels(classify []string, judge *string) error {
	if len(s.lloaModelOptions) == 0 {
		return ErrInvalidArgument{Message: "verify mode requires LLOA model allowlist (config/lloa_models.json)"}
	}
	trimmed := make([]string, 0, len(classify))
	for _, m := range classify {
		if t := strings.TrimSpace(m); t != "" {
			trimmed = append(trimmed, t)
		}
	}
	if len(trimmed) != 2 || trimmed[0] == trimmed[1] {
		return ErrInvalidArgument{Message: "verify mode requires classify_models = 2 distinct allowlisted model ids"}
	}
	for _, m := range trimmed {
		if err := s.validateLLOAModelInAllowlist(m); err != nil {
			return err
		}
	}
	if judge != nil && strings.TrimSpace(*judge) != "" {
		if err := s.validateLLOAModelInAllowlist(*judge); err != nil {
			return err
		}
	}
	return nil
}

// SetPlanReuseEnabled — plan reuse(POC-1) 활성 여부 주입. config의
// ANALYSIS_PLAN_REUSE_ENABLED를 wiring 시점에 넘긴다. 기본 false(비활성).
func (s *DatasetService) SetPlanReuseEnabled(enabled bool) {
	s.planReuseEnabled = enabled
}

// SetPythonAITaskTimeout — analyze worker 호출 HTTP timeout 주입.
// config의 PYTHON_AI_WORKER_HTTP_TIMEOUT_SEC를 wiring 시점에 넘긴다.
// 0 이하면 무시(postPythonAITask가 default로 fallback).
func (s *DatasetService) SetPythonAITaskTimeout(d time.Duration) {
	if d > 0 {
		s.pythonAITaskTimeout = d
	}
}

func (s *DatasetService) CreateDataset(projectID string, input domain.DatasetCreateRequest) (domain.Dataset, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.Dataset{}, ErrNotFound{Resource: "project"}
		}
		return domain.Dataset{}, err
	}

	name := strings.TrimSpace(input.Name)
	if name == "" {
		return domain.Dataset{}, ErrInvalidArgument{Message: "name is required"}
	}
	dataType := normalizeDatasetDataType(input.DataType, "structured")

	dataset := domain.Dataset{
		DatasetID:   id.New(),
		ProjectID:   projectID,
		Name:        name,
		Description: input.Description,
		DataType:    dataType,
		Metadata:    cloneMetadata(input.Metadata),
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.store.SaveDataset(dataset); err != nil {
		return domain.Dataset{}, err
	}
	return dataset, nil
}

// UpdateDatasetMetadata — PATCH /projects/{pid}/datasets/{did}/metadata.
// silverone 2026-05-22 (옵션 α1) — dataset-level 설정을 운영자가 갱신할 수
// 있게 한다. patch 시맨틱은 top-level key 단위 merge: 같은 key는 새 값으로
// overwrite, patch에 없는 key는 보존. nested object(예: doc_genuineness)는
// 통째 overwrite — partial merge 정책은 보수적으로 빼고, 운영자가 항상
// nested 객체 전체를 보내도록 강제.
func (s *DatasetService) UpdateDatasetMetadata(projectID, datasetID string, patch map[string]any) (domain.Dataset, error) {
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.Dataset{}, err
	}
	if patch == nil {
		return domain.Dataset{}, ErrInvalidArgument{Message: "metadata patch must be an object"}
	}
	merged := cloneMetadata(dataset.Metadata)
	if merged == nil {
		merged = map[string]any{}
	}
	for key, value := range patch {
		merged[key] = value
	}
	// 축제 메타데이터(#31)는 2026-07-01부터 project.metadata.festival(프로젝트 레벨)로
	// 이동했다. dataset.metadata에선 더 이상 검증/정규화하지 않는다(단일 source).
	dataset.Metadata = merged
	if err := s.store.SaveDataset(dataset); err != nil {
		return domain.Dataset{}, err
	}
	return dataset, nil
}

// UpdateDatasetInfo — PATCH /projects/{pid}/datasets/{did}. 데이터셋 이름/설명 수정.
// silverone 2026-06-05 — non-nil 필드만 반영. name은 trim 후 빈 문자열이면 거부.
// data_type은 기존 버전/빌드 정합성 위험으로 이 endpoint 변경 대상에서 제외.
func (s *DatasetService) UpdateDatasetInfo(projectID, datasetID string, input domain.DatasetInfoUpdateRequest) (domain.Dataset, error) {
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.Dataset{}, err
	}
	if input.Name == nil && input.Description == nil {
		return domain.Dataset{}, ErrInvalidArgument{Message: "name or description is required"}
	}
	if input.Name != nil {
		name := strings.TrimSpace(*input.Name)
		if name == "" {
			return domain.Dataset{}, ErrInvalidArgument{Message: "name must not be empty"}
		}
		dataset.Name = name
	}
	if input.Description != nil {
		description := strings.TrimSpace(*input.Description)
		dataset.Description = &description
	}
	if err := s.store.SaveDataset(dataset); err != nil {
		return domain.Dataset{}, err
	}
	return dataset, nil
}

func cloneMetadata(source map[string]any) map[string]any {
	if len(source) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func (s *DatasetService) GetDataset(projectID, datasetID string) (domain.Dataset, error) {
	dataset, err := s.store.GetDataset(projectID, datasetID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Dataset{}, ErrNotFound{Resource: "dataset"}
		}
		return domain.Dataset{}, err
	}
	return dataset, nil
}

func (s *DatasetService) ListDatasets(projectID string) (domain.DatasetListResponse, error) {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return domain.DatasetListResponse{}, ErrNotFound{Resource: "project"}
		}
		return domain.DatasetListResponse{}, err
	}
	items, err := s.store.ListDatasets(projectID)
	if err != nil {
		return domain.DatasetListResponse{}, err
	}
	return domain.DatasetListResponse{Items: items}, nil
}

func (s *DatasetService) DeleteDataset(projectID, datasetID string) error {
	if _, err := s.GetDataset(projectID, datasetID); err != nil {
		return err
	}
	if err := s.store.DeleteDataset(projectID, datasetID); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "dataset"}
		}
		return err
	}
	if err := s.removeDatasetArtifacts(projectID, datasetID); err != nil {
		return err
	}
	return nil
}

func (s *DatasetService) ActivateDatasetVersion(projectID, datasetID, datasetVersionID string) (domain.Dataset, error) {
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.Dataset{}, err
	}
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.Dataset{}, err
	}
	if version.DatasetID != dataset.DatasetID {
		return domain.Dataset{}, ErrNotFound{Resource: "dataset version"}
	}
	return s.saveDatasetActiveVersion(dataset, &version.DatasetVersionID)
}

func (s *DatasetService) DeactivateDatasetVersion(projectID, datasetID string) (domain.Dataset, error) {
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.Dataset{}, err
	}
	return s.saveDatasetActiveVersion(dataset, nil)
}

// silverone 2026-06-04 (ADR-018 β2 residue cleanup) — embedding / cluster /
// sentiment / prepare URI derive helper들은 ADR-018 β2로 제거된 build 단계
// 잔재라 호출처 0건이었다. deriveEmbeddingURI / deriveEmbeddingIndexSourceURI /
// deriveClusterURI / deriveSentimentURI / derivePrepareURI (method + 중복 package
// func)와 그것만 쓰던 datasetSourceForUnstructured를 제거했다. 살아 있는
// deriveCleanURI(clean 단계)만 남긴다.

func (s *DatasetService) deriveCleanURI(version domain.DatasetVersion) string {
	if ref := cleanArtifactRef(version); ref != "" {
		return ref
	}
	if path, ok := s.datasetArtifactPath(version, "clean", "cleaned.parquet"); ok {
		return path
	}
	return strings.TrimSpace(version.StorageURI) + ".cleaned.parquet"
}
