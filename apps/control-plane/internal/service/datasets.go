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

const (
	defaultDatasetSourceSummarySampleLimit = 5
	defaultDatasetBuildSampleRows          = 10
	maxDatasetBuildSampleRows              = 20
)

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
	// 5/11 (silverone): dataset_build HTTP 호출은 PythonBuildClient로 분리.
	// 기존 `runWorkerTask` generic helper는 dataset_build_*.go가 직접
	// 호출하지 않고 client method를 호출하도록 점진 마이그레이션 중.
	// dataset_clean이 첫 마이그레이션 대상 — 다른 build 단계는 후속 작업.
	pythonBuildClient *skills.PythonBuildClient
	// silverone 2026-05-28 subpackage pilot — 옛 dataset_prompts.go (823 lines)을
	// internal/service/datasetprompts/로 분리. DatasetService는 facade method로
	// 같은 public 시그니처를 유지하고 위임만 한다.
	prompts *datasetprompts.Service
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
func (s *DatasetService) buildClient() *skills.PythonBuildClient {
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
	dataset.Metadata = merged
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

func (s *DatasetService) deriveEmbeddingURI(version domain.DatasetVersion) string {
	// silverone 2026-05-28 (β2 cleanup PR2) — struct 필드 제거. metadata fallback.
	if ref := strings.TrimSpace(metadataString(version.Metadata, "embedding_uri", "")); ref != "" {
		return ref
	}
	if path, ok := s.datasetArtifactPath(version, "embedding", "embeddings.jsonl"); ok {
		return path
	}
	return datasetSourceForUnstructured(version) + ".embeddings.jsonl"
}

func (s *DatasetService) deriveEmbeddingIndexSourceURI(version domain.DatasetVersion) string {
	if ref := strings.TrimSpace(metadataString(version.Metadata, "embedding_index_source_ref", "")); ref != "" {
		return ref
	}
	if path, ok := s.datasetArtifactPath(version, "embedding", "embeddings.index.parquet"); ok {
		return path
	}
	return datasetSourceForUnstructured(version) + ".embeddings.index.parquet"
}

func (s *DatasetService) deriveClusterURI(version domain.DatasetVersion) string {
	if ref := strings.TrimSpace(metadataString(version.Metadata, "cluster_ref", "")); ref != "" {
		return ref
	}
	if path, ok := s.datasetArtifactPath(version, "cluster", "clusters.json"); ok {
		return path
	}
	return datasetSourceForUnstructured(version) + ".clusters.json"
}

func (s *DatasetService) deriveSentimentURI(version domain.DatasetVersion) string {
	if ref := strings.TrimSpace(metadataString(version.Metadata, "sentiment_uri", "")); ref != "" {
		return ref
	}
	if path, ok := s.datasetArtifactPath(version, "sentiment", "sentiment.parquet"); ok {
		return path
	}
	return domain.ResolveDatasetSource(version).DatasetName + ".sentiment.parquet"
}

func (s *DatasetService) deriveCleanURI(version domain.DatasetVersion) string {
	if ref := cleanArtifactRef(version); ref != "" {
		return ref
	}
	if path, ok := s.datasetArtifactPath(version, "clean", "cleaned.parquet"); ok {
		return path
	}
	return strings.TrimSpace(version.StorageURI) + ".cleaned.parquet"
}

func (s *DatasetService) derivePrepareURI(version domain.DatasetVersion) string {
	if ref := strings.TrimSpace(metadataString(version.Metadata, "prepare_uri", "")); ref != "" {
		return ref
	}
	if path, ok := s.datasetArtifactPath(version, "prepare", "prepared.parquet"); ok {
		return path
	}
	return strings.TrimSpace(version.StorageURI) + ".prepared.parquet"
}

func deriveEmbeddingURI(version domain.DatasetVersion) string {
	if ref := strings.TrimSpace(metadataString(version.Metadata, "embedding_uri", "")); ref != "" {
		return ref
	}
	return datasetSourceForUnstructured(version) + ".embeddings.jsonl"
}

func deriveSentimentURI(version domain.DatasetVersion) string {
	if ref := strings.TrimSpace(metadataString(version.Metadata, "sentiment_uri", "")); ref != "" {
		return ref
	}
	return domain.ResolveDatasetSource(version).DatasetName + ".sentiment.parquet"
}

func derivePrepareURI(version domain.DatasetVersion) string {
	if ref := strings.TrimSpace(metadataString(version.Metadata, "prepare_uri", "")); ref != "" {
		return ref
	}
	return strings.TrimSpace(version.StorageURI) + ".prepared.parquet"
}
