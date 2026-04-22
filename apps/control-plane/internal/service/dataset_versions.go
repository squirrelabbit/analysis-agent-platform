package service

import (
	"fmt"
	"io"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"
)

func (s *DatasetService) CreateDatasetVersion(projectID, datasetID string, input domain.DatasetVersionCreateRequest) (domain.DatasetVersion, error) {
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}

	storageURI := strings.TrimSpace(input.StorageURI)
	if storageURI == "" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "storage_uri is required"}
	}

	version, err := s.buildDatasetVersionRecord(projectID, dataset, storageURI, input)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	if shouldActivateDatasetVersionOnCreate(input.ActivateOnCreate) {
		if _, err := s.saveDatasetActiveVersion(dataset, &version.DatasetVersionID); err != nil {
			return domain.DatasetVersion{}, err
		}
	}
	_ = s.maybeRunEagerClean(projectID, dataset.DatasetID, version)
	return s.GetDatasetVersion(projectID, dataset.DatasetID, version.DatasetVersionID)
}

func (s *DatasetService) UploadDatasetVersion(projectID, datasetID string, input domain.DatasetVersionCreateRequest, originalName string, contentType string, reader io.Reader) (domain.DatasetVersion, error) {
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	if reader == nil {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "file is required"}
	}

	versionID := id.New()
	storedPath, uploadMetadata, err := s.persistUploadedDataset(projectID, datasetID, versionID, originalName, contentType, reader)
	if err != nil {
		return domain.DatasetVersion{}, err
	}

	if input.Metadata == nil {
		input.Metadata = map[string]any{}
	}
	input.Metadata = mergeStringAny(input.Metadata, map[string]any{
		"storage_backend": "local_fs",
		"storage_scope":   "dataset_upload",
		"upload":          uploadMetadata,
	})
	input.StorageURI = storedPath

	version, err := s.buildDatasetVersionRecord(projectID, dataset, storedPath, input)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	version.DatasetVersionID = versionID
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	if shouldActivateDatasetVersionOnCreate(input.ActivateOnCreate) {
		if _, err := s.saveDatasetActiveVersion(dataset, &version.DatasetVersionID); err != nil {
			return domain.DatasetVersion{}, err
		}
	}
	_ = s.maybeRunEagerClean(projectID, dataset.DatasetID, version)
	return s.GetDatasetVersion(projectID, dataset.DatasetID, version.DatasetVersionID)
}

func (s *DatasetService) buildDatasetVersionRecord(projectID string, dataset domain.Dataset, storageURI string, input domain.DatasetVersionCreateRequest) (domain.DatasetVersion, error) {
	dataType := normalizeDatasetDataType(input.DataType, dataset.DataType)
	metadata := input.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}
	profile := s.resolveDatasetProfile(dataType, input.Profile)
	if profile != nil && strings.TrimSpace(profile.ProfileID) != "" {
		metadata["profile_id"] = profile.ProfileID
	}
	prepareLLMMode, err := normalizeDatasetLLMMode(input.PrepareLLMMode, "prepare_llm_mode")
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	sentimentLLMMode, err := normalizeDatasetLLMMode(input.SentimentLLMMode, "sentiment_llm_mode")
	if err != nil {
		return domain.DatasetVersion{}, err
	}

	prepareRequired := defaultPrepareRequired(dataType, input.PrepareRequired)
	prepareStatus := "not_applicable"
	if dataType == "unstructured" || dataType == "mixed" || dataType == "both" {
		prepareStatus = "not_requested"
		if prepareRequired {
			prepareStatus = "not_requested"
		}
	}

	sentimentRequired := input.SentimentRequired != nil && *input.SentimentRequired
	sentimentStatus := "not_applicable"
	if dataType == "unstructured" || dataType == "mixed" || dataType == "both" {
		sentimentStatus = "not_requested"
		if sentimentRequired {
			sentimentStatus = "queued"
		}
	}

	embeddingRequired := input.EmbeddingRequired != nil && *input.EmbeddingRequired
	embeddingStatus := "not_requested"
	if embeddingRequired {
		embeddingStatus = "queued"
	}

	version := domain.DatasetVersion{
		DatasetVersionID: id.New(),
		DatasetID:        dataset.DatasetID,
		ProjectID:        projectID,
		StorageURI:       storageURI,
		DataType:         dataType,
		RecordCount:      input.RecordCount,
		Metadata:         metadata,
		Profile:          profile,
		PrepareStatus:    prepareStatus,
		PrepareLLMMode:   prepareLLMMode,
		PrepareModel:     input.PrepareModel,
		SentimentStatus:  sentimentStatus,
		SentimentLLMMode: sentimentLLMMode,
		SentimentModel:   input.SentimentModel,
		EmbeddingStatus:  embeddingStatus,
		EmbeddingModel:   input.EmbeddingModel,
		CreatedAt:        time.Now().UTC(),
	}
	if input.PrepareModel != nil && strings.TrimSpace(*input.PrepareModel) == "" {
		version.PrepareModel = nil
	}
	if input.SentimentModel != nil && strings.TrimSpace(*input.SentimentModel) == "" {
		version.SentimentModel = nil
	}
	if input.EmbeddingModel != nil && strings.TrimSpace(*input.EmbeddingModel) == "" {
		version.EmbeddingModel = nil
	}
	if prepareRequired {
		textColumns := metadataStringList(version.Metadata, "text_columns")
		if len(textColumns) == 0 {
			return domain.DatasetVersion{}, ErrInvalidArgument{Message: "metadata.text_columns is required when prepare_required is true"}
		}
		version.Metadata["text_columns"] = textColumns
		version.Metadata["prepare_required"] = true
	}
	if dataType == "unstructured" || dataType == "mixed" || dataType == "both" {
		if _, ok := version.Metadata["clean_status"]; !ok {
			version.Metadata["clean_status"] = "not_requested"
		}
	}
	if sentimentRequired {
		version.Metadata["sentiment_required"] = true
	}
	return version, nil
}

func (s *DatasetService) maybeRunEagerClean(projectID, datasetID string, version domain.DatasetVersion) domain.DatasetVersion {
	if strings.TrimSpace(s.pythonAIWorkerURL) == "" {
		return version
	}
	if !requiresClean(version) || isCleanReady(version) {
		return version
	}
	if len(resolveDatasetBuildTextSelection(version.Metadata, nil).Columns) == 0 {
		return version
	}
	if _, err := s.CreateCleanJob(projectID, datasetID, version.DatasetVersionID, domain.DatasetCleanRequest{}, "dataset_version_create"); err == nil {
		latest, getErr := s.GetDatasetVersion(projectID, datasetID, version.DatasetVersionID)
		if getErr == nil {
			return latest
		}
	}
	return version
}

func (s *DatasetService) maybeRunEagerSentiment(projectID, datasetID string, version domain.DatasetVersion) domain.DatasetVersion {
	if strings.TrimSpace(s.pythonAIWorkerURL) == "" {
		return version
	}
	if !requiresSentiment(version) || !isPrepareReady(version) {
		return version
	}
	if _, err := s.CreateSentimentJob(projectID, datasetID, version.DatasetVersionID, domain.DatasetSentimentBuildRequest{}, "dataset_prepare_complete"); err == nil {
		latest, getErr := s.GetDatasetVersion(projectID, datasetID, version.DatasetVersionID)
		if getErr == nil {
			return latest
		}
	}
	return version
}

func (s *DatasetService) GetDatasetVersion(projectID, datasetID, datasetVersionID string) (domain.DatasetVersion, error) {
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	version, err := s.store.GetDatasetVersion(projectID, datasetVersionID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.DatasetVersion{}, ErrNotFound{Resource: "dataset version"}
		}
		return domain.DatasetVersion{}, err
	}
	if version.DatasetID != datasetID {
		return domain.DatasetVersion{}, ErrNotFound{Resource: "dataset version"}
	}
	enrichDatasetVersionView(&version)
	version.SourceSummary = loadDatasetSourceSummary(version.StorageURI, defaultDatasetSourceSummarySampleLimit)
	buildJobs, err := s.latestDatasetVersionBuildJobStatuses(projectID, version)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	version.BuildJobs = buildJobs
	markDatasetVersionActive(&version, dataset)
	return version, nil
}

func (s *DatasetService) ListDatasetVersions(projectID, datasetID string) (domain.DatasetVersionListResponse, error) {
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.DatasetVersionListResponse{}, err
	}
	items, err := s.store.ListDatasetVersions(projectID, datasetID)
	if err != nil {
		return domain.DatasetVersionListResponse{}, err
	}
	for index := range items {
		enrichDatasetVersionView(&items[index])
		markDatasetVersionActive(&items[index], dataset)
	}
	return domain.DatasetVersionListResponse{Items: items}, nil
}

func (s *DatasetService) DeleteDatasetVersion(projectID, datasetID, datasetVersionID string) error {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return err
	}
	if err := s.store.DeleteDatasetVersion(projectID, datasetID, version.DatasetVersionID); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "dataset version"}
		}
		return err
	}
	if err := s.removeDatasetVersionArtifacts(projectID, datasetID, version.DatasetVersionID); err != nil {
		return err
	}
	return nil
}

func normalizeDatasetDataType(value *string, fallback string) string {
	dataType := strings.TrimSpace(fallback)
	if value != nil && strings.TrimSpace(*value) != "" {
		dataType = strings.TrimSpace(*value)
	}
	if dataType == "" {
		dataType = "structured"
	}
	return dataType
}

func normalizeDatasetLLMMode(value *string, fieldName string) (string, error) {
	if value == nil {
		return datasetLLMModeDefault, nil
	}
	mode := strings.ToLower(strings.TrimSpace(*value))
	if mode == "" {
		return datasetLLMModeDefault, nil
	}
	switch mode {
	case datasetLLMModeDefault, datasetLLMModeEnabled, datasetLLMModeDisabled:
		return mode, nil
	default:
		return "", ErrInvalidArgument{Message: fmt.Sprintf("%s must be one of default, enabled, disabled", fieldName)}
	}
}
