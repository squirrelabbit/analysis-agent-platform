package service

import (
	"net/http"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"
	"analysis-support-platform/control-plane/internal/workflows"
)

const DefaultEmbeddingModel = "intfloat/multilingual-e5-small"

const (
	defaultClusterMembersLimit             = 50
	maxClusterMembersLimit                 = 500
	defaultDatasetSourceSummarySampleLimit = 5
	defaultPreparePreviewLimit             = 10
	maxPreparePreviewLimit                 = 20
	defaultSentimentPreviewLimit           = 10
	maxSentimentPreviewLimit               = 20
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
	promptTemplatesDir  string
	profileRegistry     *datasetProfileRegistry
	buildJobStarter     workflows.Starter
	httpClient          *http.Client
}

func NewDatasetService(repository store.Repository, pythonAIWorkerURL string, uploadRoot string, artifactRoot string) *DatasetService {
	return &DatasetService{
		store:             repository,
		pythonAIWorkerURL: pythonAIWorkerURL,
		uploadRoot:        strings.TrimSpace(uploadRoot),
		artifactRoot:      strings.TrimSpace(artifactRoot),
		httpClient:        &http.Client{},
	}
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
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.store.SaveDataset(dataset); err != nil {
		return domain.Dataset{}, err
	}
	return dataset, nil
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
	if version.EmbeddingURI != nil && strings.TrimSpace(*version.EmbeddingURI) != "" {
		return strings.TrimSpace(*version.EmbeddingURI)
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
	if version.SentimentURI != nil && strings.TrimSpace(*version.SentimentURI) != "" {
		return strings.TrimSpace(*version.SentimentURI)
	}
	if path, ok := s.datasetArtifactPath(version, "sentiment", "sentiment.parquet"); ok {
		return path
	}
	return domain.ResolveDatasetSource(version).DatasetName + ".sentiment.parquet"
}

func (s *DatasetService) deriveCleanURI(version domain.DatasetVersion) string {
	if ref := strings.TrimSpace(metadataString(version.Metadata, "cleaned_ref", "")); ref != "" {
		return ref
	}
	if path, ok := s.datasetArtifactPath(version, "clean", "cleaned.parquet"); ok {
		return path
	}
	return strings.TrimSpace(version.StorageURI) + ".cleaned.parquet"
}

func (s *DatasetService) derivePrepareURI(version domain.DatasetVersion) string {
	if version.PrepareURI != nil && strings.TrimSpace(*version.PrepareURI) != "" {
		return strings.TrimSpace(*version.PrepareURI)
	}
	if path, ok := s.datasetArtifactPath(version, "prepare", "prepared.parquet"); ok {
		return path
	}
	return strings.TrimSpace(version.StorageURI) + ".prepared.parquet"
}

func deriveEmbeddingURI(version domain.DatasetVersion) string {
	if version.EmbeddingURI != nil && strings.TrimSpace(*version.EmbeddingURI) != "" {
		return strings.TrimSpace(*version.EmbeddingURI)
	}
	return datasetSourceForUnstructured(version) + ".embeddings.jsonl"
}

func deriveSentimentURI(version domain.DatasetVersion) string {
	if version.SentimentURI != nil && strings.TrimSpace(*version.SentimentURI) != "" {
		return strings.TrimSpace(*version.SentimentURI)
	}
	return domain.ResolveDatasetSource(version).DatasetName + ".sentiment.parquet"
}

func derivePrepareURI(version domain.DatasetVersion) string {
	if version.PrepareURI != nil && strings.TrimSpace(*version.PrepareURI) != "" {
		return strings.TrimSpace(*version.PrepareURI)
	}
	return strings.TrimSpace(version.StorageURI) + ".prepared.parquet"
}
