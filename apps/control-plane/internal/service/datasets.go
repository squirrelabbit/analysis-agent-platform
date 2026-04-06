package service

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"

	_ "github.com/marcboeker/go-duckdb"
)

const DefaultEmbeddingModel = "intfloat/multilingual-e5-small"
const tokenProjectionVectorDim = 64

type DatasetService struct {
	store             store.Repository
	pythonAIWorkerURL string
	uploadRoot        string
	artifactRoot      string
	httpClient        *http.Client
}

type workerTaskResponse struct {
	Notes    []string       `json:"notes"`
	Artifact map[string]any `json:"artifact"`
}

func NewDatasetService(repository store.Repository, pythonAIWorkerURL string, uploadRoot string, artifactRoot string) *DatasetService {
	return &DatasetService{
		store:             repository,
		pythonAIWorkerURL: pythonAIWorkerURL,
		uploadRoot:        strings.TrimSpace(uploadRoot),
		artifactRoot:      strings.TrimSpace(artifactRoot),
		httpClient:        &http.Client{Timeout: 60 * time.Second},
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

func (s *DatasetService) CreateDatasetVersion(projectID, datasetID string, input domain.DatasetVersionCreateRequest) (domain.DatasetVersion, error) {
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}

	storageURI := strings.TrimSpace(input.StorageURI)
	if storageURI == "" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "storage_uri is required"}
	}

	version := s.buildDatasetVersionRecord(projectID, dataset, storageURI, input)
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
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

	version := s.buildDatasetVersionRecord(projectID, dataset, storedPath, input)
	version.DatasetVersionID = versionID
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
}

func (s *DatasetService) buildDatasetVersionRecord(projectID string, dataset domain.Dataset, storageURI string, input domain.DatasetVersionCreateRequest) domain.DatasetVersion {
	dataType := normalizeDatasetDataType(input.DataType, dataset.DataType)
	metadata := input.Metadata
	if metadata == nil {
		metadata = map[string]any{}
	}
	profile := normalizeDatasetProfile(input.Profile)
	if profile != nil && strings.TrimSpace(profile.ProfileID) != "" {
		metadata["profile_id"] = profile.ProfileID
	}

	prepareRequired := defaultPrepareRequired(dataType, input.PrepareRequired)
	prepareStatus := "not_applicable"
	if dataType == "unstructured" || dataType == "mixed" || dataType == "both" {
		prepareStatus = "not_requested"
		if prepareRequired {
			prepareStatus = "queued"
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
		PrepareModel:     input.PrepareModel,
		SentimentStatus:  sentimentStatus,
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
		version.Metadata["prepare_required"] = true
	}
	if sentimentRequired {
		version.Metadata["sentiment_required"] = true
	}
	return version
}

func (s *DatasetService) GetDatasetVersion(projectID, datasetID, datasetVersionID string) (domain.DatasetVersion, error) {
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
	return version, nil
}

func (s *DatasetService) BuildPrepare(projectID, datasetID, datasetVersionID string, input domain.DatasetPrepareRequest) (domain.DatasetVersion, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	if version.DataType == "structured" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "dataset prepare requires unstructured or mixed dataset version"}
	}

	force := input.Force != nil && *input.Force
	if version.PrepareStatus == "ready" && version.PrepareURI != nil && !force {
		return version, nil
	}

	textColumn := metadataString(version.Metadata, "text_column", "text")
	if input.TextColumn != nil && strings.TrimSpace(*input.TextColumn) != "" {
		textColumn = strings.TrimSpace(*input.TextColumn)
	}

	outputPath := s.derivePrepareURI(version)
	if input.OutputPath != nil && strings.TrimSpace(*input.OutputPath) != "" {
		outputPath = strings.TrimSpace(*input.OutputPath)
	}

	version.PrepareStatus = "preparing"
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.PrepareURI = &outputPath
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	version.Metadata["raw_text_column"] = textColumn
	if input.Model != nil && strings.TrimSpace(*input.Model) != "" {
		model := strings.TrimSpace(*input.Model)
		version.PrepareModel = &model
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	payload := map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"dataset_name":       version.StorageURI,
		"text_column":        textColumn,
		"output_path":        outputPath,
	}
	if version.Profile != nil {
		if len(version.Profile.RegexRuleNames) > 0 {
			payload["regex_rule_names"] = append([]string(nil), version.Profile.RegexRuleNames...)
		}
		if version.Profile.PreparePromptVersion != nil && strings.TrimSpace(*version.Profile.PreparePromptVersion) != "" {
			payload["prepare_prompt_version"] = strings.TrimSpace(*version.Profile.PreparePromptVersion)
		}
	}
	if version.PrepareModel != nil && strings.TrimSpace(*version.PrepareModel) != "" {
		payload["model"] = strings.TrimSpace(*version.PrepareModel)
	}

	response, err := s.runWorkerTask(context.Background(), "/tasks/dataset_prepare", payload)
	if err != nil {
		version.PrepareStatus = "failed"
		version.Metadata["prepare_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	now := time.Now().UTC()
	version.PrepareStatus = "ready"
	version.PreparedAt = &now
	preparedTextColumn := artifactString(response.Artifact, "prepared_text_column")
	if preparedTextColumn == "" {
		preparedTextColumn = "normalized_text"
	}
	prepareRef := artifactString(response.Artifact, "prepared_ref")
	if prepareRef == "" {
		prepareRef = artifactString(response.Artifact, "prepare_uri")
	}
	prepareFormat := artifactString(response.Artifact, "prepare_format")
	if prepareFormat == "" && prepareRef != "" {
		prepareFormat = inferArtifactFormat(prepareRef, "jsonl")
	}
	prepareMetadata := map[string]any{
		"prepare_notes":        response.Notes,
		"raw_text_column":      textColumn,
		"prepared_text_column": preparedTextColumn,
	}
	if prepareRef != "" {
		prepareMetadata["prepared_ref"] = prepareRef
	}
	if prepareFormat != "" {
		prepareMetadata["prepared_format"] = prepareFormat
	}
	if rowIDColumn := artifactString(response.Artifact, "row_id_column"); rowIDColumn != "" {
		prepareMetadata["row_id_column"] = rowIDColumn
	}
	if contractVersion := artifactString(response.Artifact, "storage_contract_version"); contractVersion != "" {
		prepareMetadata["storage_contract_version"] = contractVersion
	}
	if usage := artifactMap(response.Artifact, "usage"); len(usage) > 0 {
		prepareMetadata["prepare_usage"] = usage
	}
	version.Metadata = mergeStringAny(version.Metadata, prepareMetadata)
	if promptVersion := artifactString(response.Artifact, "prepare_prompt_version"); promptVersion != "" {
		version.PreparePromptVer = &promptVersion
	}
	if prepareURI := artifactString(response.Artifact, "prepare_uri"); prepareURI != "" {
		version.PrepareURI = &prepareURI
	}
	if prepareModel := artifactString(response.Artifact, "prepare_model"); prepareModel != "" {
		version.PrepareModel = &prepareModel
	}
	if summary, ok := response.Artifact["summary"].(map[string]any); ok {
		version.Metadata = mergeStringAny(version.Metadata, map[string]any{
			"prepare_summary": summary,
		})
		if value, ok := summary["output_row_count"]; ok {
			if intValue, ok := anyToInt(value); ok {
				version.RecordCount = &intValue
			}
		}
	}
	delete(version.Metadata, "prepare_error")
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
}

func (s *DatasetService) BuildEmbeddings(projectID, datasetID, datasetVersionID string, input domain.DatasetEmbeddingBuildRequest) (domain.DatasetVersion, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	if version.DataType == "structured" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "embeddings require unstructured or mixed dataset version"}
	}
	if requiresPrepare(version) && !isPrepareReady(version) {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "dataset prepare must be ready before embeddings"}
	}

	force := input.Force != nil && *input.Force
	if embeddingBuildReady(version) && !force {
		return version, nil
	}

	textColumn := defaultPreparedTextColumn(version)
	if input.TextColumn != nil && strings.TrimSpace(*input.TextColumn) != "" {
		requestedTextColumn := strings.TrimSpace(*input.TextColumn)
		rawTextColumn := metadataString(version.Metadata, "raw_text_column", metadataString(version.Metadata, "text_column", "text"))
		if !isPrepareReady(version) || requestedTextColumn != rawTextColumn {
			textColumn = requestedTextColumn
		}
	}
	datasetName := datasetSourceForUnstructured(version)

	version.EmbeddingStatus = "building"
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["text_column"] = textColumn
	version.Metadata["embedding_dataset_name"] = datasetName
	indexOutputPath := s.deriveEmbeddingIndexSourceURI(version)
	debugExportJSONL := input.DebugExportJSONL != nil && *input.DebugExportJSONL
	outputPath := ""
	if debugExportJSONL {
		outputPath = s.deriveEmbeddingURI(version)
		version.EmbeddingURI = &outputPath
		if err := ensureParentDir(outputPath); err != nil {
			return domain.DatasetVersion{}, err
		}
	} else {
		version.EmbeddingURI = nil
	}
	if err := ensureParentDir(indexOutputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	if input.EmbeddingModel != nil {
		requestedModel := strings.TrimSpace(*input.EmbeddingModel)
		if requestedModel == "" {
			version.EmbeddingModel = nil
		} else {
			version.EmbeddingModel = &requestedModel
		}
	}
	if version.EmbeddingModel == nil {
		if version.Profile != nil && version.Profile.EmbeddingModel != nil && strings.TrimSpace(*version.Profile.EmbeddingModel) != "" {
			model := strings.TrimSpace(*version.Profile.EmbeddingModel)
			version.EmbeddingModel = &model
		} else {
			model := DefaultEmbeddingModel
			version.EmbeddingModel = &model
		}
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	payload := map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"dataset_name":       datasetName,
		"text_column":        textColumn,
		"index_output_path":  indexOutputPath,
		"embedding_model":    derefString(version.EmbeddingModel),
	}
	if debugExportJSONL {
		payload["output_path"] = outputPath
	}
	response, err := s.runWorkerTask(context.Background(), "/tasks/embedding", payload)
	if err != nil {
		version.EmbeddingStatus = "failed"
		version.Metadata["embedding_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	now := time.Now().UTC()
	version.EmbeddingStatus = "ready"
	version.ReadyAt = &now
	embeddingRef := artifactString(response.Artifact, "embedding_ref")
	if embeddingRef == "" {
		embeddingRef = artifactString(response.Artifact, "embedding_uri")
	}
	embeddingFormat := artifactString(response.Artifact, "embedding_format")
	if embeddingFormat == "" && embeddingRef != "" {
		embeddingFormat = inferArtifactFormat(embeddingRef, "")
	}
	embeddingIndexSourceRef := artifactString(response.Artifact, "embedding_index_source_ref")
	if embeddingIndexSourceRef == "" {
		embeddingIndexSourceRef = indexOutputPath
	}
	embeddingIndexSourceFormat := artifactString(response.Artifact, "embedding_index_source_format")
	if embeddingIndexSourceFormat == "" && embeddingIndexSourceRef != "" {
		embeddingIndexSourceFormat = inferArtifactFormat(embeddingIndexSourceRef, "parquet")
	}
	embeddingMetadata := map[string]any{
		"text_column":                  textColumn,
		"embedding_notes":              response.Notes,
		"embedding_debug_export_jsonl": debugExportJSONL,
	}
	if embeddingRef != "" {
		embeddingMetadata["embedding_ref"] = embeddingRef
	} else {
		delete(version.Metadata, "embedding_ref")
	}
	if embeddingFormat != "" {
		embeddingMetadata["embedding_format"] = embeddingFormat
	} else {
		delete(version.Metadata, "embedding_format")
	}
	if embeddingIndexSourceRef != "" {
		embeddingMetadata["embedding_index_source_ref"] = embeddingIndexSourceRef
	}
	if embeddingIndexSourceFormat != "" {
		embeddingMetadata["embedding_index_source_format"] = embeddingIndexSourceFormat
	}
	if chunkRef := artifactString(response.Artifact, "chunk_ref"); chunkRef != "" {
		embeddingMetadata["chunk_ref"] = chunkRef
	}
	if chunkFormat := artifactString(response.Artifact, "chunk_format"); chunkFormat != "" {
		embeddingMetadata["chunk_format"] = chunkFormat
	}
	if rowIDColumn := artifactString(response.Artifact, "row_id_column"); rowIDColumn != "" {
		embeddingMetadata["row_id_column"] = rowIDColumn
	}
	if chunkIDColumn := artifactString(response.Artifact, "chunk_id_column"); chunkIDColumn != "" {
		embeddingMetadata["chunk_id_column"] = chunkIDColumn
	}
	if chunkIndexColumn := artifactString(response.Artifact, "chunk_index_column"); chunkIndexColumn != "" {
		embeddingMetadata["chunk_index_column"] = chunkIndexColumn
	}
	if chunkTextColumn := artifactString(response.Artifact, "chunk_text_column"); chunkTextColumn != "" {
		embeddingMetadata["chunk_text_column"] = chunkTextColumn
	}
	if chunkingStrategy := artifactString(response.Artifact, "chunking_strategy"); chunkingStrategy != "" {
		embeddingMetadata["chunking_strategy"] = chunkingStrategy
	}
	if embeddingProvider := artifactString(response.Artifact, "embedding_provider"); embeddingProvider != "" {
		embeddingMetadata["embedding_provider"] = embeddingProvider
	}
	if embeddingRepresentation := artifactString(response.Artifact, "embedding_representation"); embeddingRepresentation != "" {
		embeddingMetadata["embedding_representation"] = embeddingRepresentation
	}
	if contractVersion := artifactString(response.Artifact, "storage_contract_version"); contractVersion != "" {
		embeddingMetadata["storage_contract_version"] = contractVersion
	}
	if usage := artifactMap(response.Artifact, "usage"); len(usage) > 0 {
		embeddingMetadata["embedding_usage"] = usage
	}
	version.Metadata = mergeStringAny(version.Metadata, embeddingMetadata)
	if value, ok := response.Artifact["document_count"]; ok {
		version.Metadata["document_count"] = value
	}
	if value, ok := response.Artifact["chunk_count"]; ok {
		version.Metadata["chunk_count"] = value
	}
	if value, ok := response.Artifact["source_row_count"]; ok {
		version.Metadata["source_row_count"] = value
	}
	if value, ok := response.Artifact["embedding_vector_dim"]; ok {
		version.Metadata["embedding_vector_dim"] = value
	}
	if value, ok := response.Artifact["embedding_uri"].(string); ok && strings.TrimSpace(value) != "" {
		version.EmbeddingURI = &value
	} else {
		version.EmbeddingURI = nil
	}
	if value, ok := response.Artifact["embedding_model"].(string); ok && strings.TrimSpace(value) != "" {
		version.EmbeddingModel = &value
	}
	indexEmbeddingRef := resolveReadableArtifactRef(embeddingIndexSourceRef, embeddingRef, outputPath)
	if err := s.syncEmbeddingIndex(version, indexEmbeddingRef, artifactString(response.Artifact, "chunk_ref")); err != nil {
		version.EmbeddingStatus = "failed"
		version.Metadata["embedding_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}
	delete(version.Metadata, "embedding_error")
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
}

func (s *DatasetService) BuildSentiment(projectID, datasetID, datasetVersionID string, input domain.DatasetSentimentBuildRequest) (domain.DatasetVersion, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	if version.DataType == "structured" {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "sentiment labeling requires unstructured or mixed dataset version"}
	}
	if requiresPrepare(version) && !isPrepareReady(version) {
		return domain.DatasetVersion{}, ErrInvalidArgument{Message: "dataset prepare must be ready before sentiment labeling"}
	}

	force := input.Force != nil && *input.Force
	if version.SentimentStatus == "ready" && version.SentimentURI != nil && !force {
		return version, nil
	}

	textColumn := defaultPreparedTextColumn(version)
	if input.TextColumn != nil && strings.TrimSpace(*input.TextColumn) != "" {
		requestedTextColumn := strings.TrimSpace(*input.TextColumn)
		rawTextColumn := metadataString(version.Metadata, "raw_text_column", metadataString(version.Metadata, "text_column", "text"))
		if !isPrepareReady(version) || requestedTextColumn != rawTextColumn {
			textColumn = requestedTextColumn
		}
	}
	datasetName := datasetSourceForUnstructured(version)
	outputPath := s.deriveSentimentURI(version)
	if input.OutputPath != nil && strings.TrimSpace(*input.OutputPath) != "" {
		outputPath = strings.TrimSpace(*input.OutputPath)
	}

	version.SentimentStatus = "labeling"
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.SentimentURI = &outputPath
	if err := ensureParentDir(outputPath); err != nil {
		return domain.DatasetVersion{}, err
	}
	version.Metadata["sentiment_dataset_name"] = datasetName
	version.Metadata["sentiment_text_column"] = textColumn
	if input.Model != nil && strings.TrimSpace(*input.Model) != "" {
		model := strings.TrimSpace(*input.Model)
		version.SentimentModel = &model
	}
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}

	payload := map[string]any{
		"dataset_version_id": version.DatasetVersionID,
		"dataset_name":       datasetName,
		"text_column":        textColumn,
		"output_path":        outputPath,
	}
	if version.Profile != nil && version.Profile.SentimentPromptVersion != nil && strings.TrimSpace(*version.Profile.SentimentPromptVersion) != "" {
		payload["sentiment_prompt_version"] = strings.TrimSpace(*version.Profile.SentimentPromptVersion)
	}
	if version.SentimentModel != nil && strings.TrimSpace(*version.SentimentModel) != "" {
		payload["model"] = strings.TrimSpace(*version.SentimentModel)
	}

	response, err := s.runWorkerTask(context.Background(), "/tasks/sentiment_label", payload)
	if err != nil {
		version.SentimentStatus = "failed"
		version.Metadata["sentiment_error"] = err.Error()
		_ = s.store.SaveDatasetVersion(version)
		return domain.DatasetVersion{}, err
	}

	now := time.Now().UTC()
	version.SentimentStatus = "ready"
	version.SentimentLabeledAt = &now
	version.ReadyAt = &now
	sentimentRef := artifactString(response.Artifact, "sentiment_ref")
	if sentimentRef == "" {
		sentimentRef = artifactString(response.Artifact, "sentiment_uri")
	}
	sentimentFormat := artifactString(response.Artifact, "sentiment_format")
	if sentimentFormat == "" && sentimentRef != "" {
		sentimentFormat = inferArtifactFormat(sentimentRef, "jsonl")
	}
	sentimentMetadata := map[string]any{
		"sentiment_notes":             response.Notes,
		"sentiment_text_column":       textColumn,
		"sentiment_label_column":      artifactString(response.Artifact, "sentiment_label_column"),
		"sentiment_reason_column":     artifactString(response.Artifact, "sentiment_reason_column"),
		"sentiment_confidence_column": artifactString(response.Artifact, "sentiment_confidence_column"),
	}
	if sentimentRef != "" {
		sentimentMetadata["sentiment_ref"] = sentimentRef
	}
	if sentimentFormat != "" {
		sentimentMetadata["sentiment_format"] = sentimentFormat
	}
	if rowIDColumn := artifactString(response.Artifact, "row_id_column"); rowIDColumn != "" {
		sentimentMetadata["row_id_column"] = rowIDColumn
	}
	if contractVersion := artifactString(response.Artifact, "storage_contract_version"); contractVersion != "" {
		sentimentMetadata["storage_contract_version"] = contractVersion
	}
	if usage := artifactMap(response.Artifact, "usage"); len(usage) > 0 {
		sentimentMetadata["sentiment_usage"] = usage
	}
	version.Metadata = mergeStringAny(version.Metadata, sentimentMetadata)
	if sentimentURI := artifactString(response.Artifact, "sentiment_uri"); sentimentURI != "" {
		version.SentimentURI = &sentimentURI
	}
	if sentimentModel := artifactString(response.Artifact, "sentiment_model"); sentimentModel != "" {
		version.SentimentModel = &sentimentModel
	}
	if promptVersion := artifactString(response.Artifact, "sentiment_prompt_version"); promptVersion != "" {
		version.SentimentPromptVer = &promptVersion
	}
	if summary, ok := response.Artifact["summary"].(map[string]any); ok {
		version.Metadata["sentiment_summary"] = summary
	}
	delete(version.Metadata, "sentiment_error")
	if err := s.store.SaveDatasetVersion(version); err != nil {
		return domain.DatasetVersion{}, err
	}
	return version, nil
}

func (s *DatasetService) runWorkerTask(ctx context.Context, taskPath string, payload map[string]any) (workerTaskResponse, error) {
	baseURL := strings.TrimRight(strings.TrimSpace(s.pythonAIWorkerURL), "/")
	if baseURL == "" {
		return workerTaskResponse{}, errors.New("python ai worker url is required")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return workerTaskResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+taskPath, bytes.NewReader(body))
	if err != nil {
		return workerTaskResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return workerTaskResponse{}, err
	}
	defer resp.Body.Close()

	var decoded workerTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return workerTaskResponse{}, err
	}
	if resp.StatusCode >= 300 {
		return workerTaskResponse{}, fmt.Errorf("worker task %s returned %d", taskPath, resp.StatusCode)
	}
	return decoded, nil
}

func (s *DatasetService) persistUploadedDataset(projectID, datasetID, datasetVersionID, originalName, contentType string, reader io.Reader) (string, map[string]any, error) {
	root := strings.TrimSpace(s.uploadRoot)
	if root == "" {
		return "", nil, errors.New("upload root is required")
	}

	filename := sanitizeFilename(originalName)
	if filename == "" {
		filename = "dataset-upload.bin"
	}
	targetDir := filepath.Join(root, "projects", projectID, "datasets", datasetID, "versions", datasetVersionID, "source")
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		return "", nil, err
	}
	targetPath := filepath.Join(targetDir, filename)

	file, err := os.Create(targetPath)
	if err != nil {
		return "", nil, err
	}
	defer file.Close()

	written, err := io.Copy(file, reader)
	if err != nil {
		return "", nil, err
	}

	absolutePath, err := filepath.Abs(targetPath)
	if err != nil {
		return "", nil, err
	}
	return absolutePath, map[string]any{
		"original_filename": strings.TrimSpace(originalName),
		"stored_filename":   filename,
		"content_type":      strings.TrimSpace(contentType),
		"byte_size":         written,
		"uploaded_at":       time.Now().UTC(),
	}, nil
}

func (s *DatasetService) datasetArtifactPath(version domain.DatasetVersion, scope string, filename string) (string, bool) {
	root := strings.TrimSpace(s.artifactRoot)
	if root == "" {
		return "", false
	}
	path := filepath.Join(root, "projects", version.ProjectID, "datasets", version.DatasetID, "versions", version.DatasetVersionID, scope, filename)
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return path, true
	}
	return absolutePath, true
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

func (s *DatasetService) deriveSentimentURI(version domain.DatasetVersion) string {
	if version.SentimentURI != nil && strings.TrimSpace(*version.SentimentURI) != "" {
		return strings.TrimSpace(*version.SentimentURI)
	}
	if path, ok := s.datasetArtifactPath(version, "sentiment", "sentiment.parquet"); ok {
		return path
	}
	base := strings.TrimSpace(version.StorageURI)
	if requiresPrepare(version) {
		base = s.derivePrepareURI(version)
	}
	return base + ".sentiment.parquet"
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
	base := strings.TrimSpace(version.StorageURI)
	if requiresPrepare(version) {
		base = derivePrepareURI(version)
	}
	return base + ".sentiment.parquet"
}

func derivePrepareURI(version domain.DatasetVersion) string {
	if version.PrepareURI != nil && strings.TrimSpace(*version.PrepareURI) != "" {
		return strings.TrimSpace(*version.PrepareURI)
	}
	return strings.TrimSpace(version.StorageURI) + ".prepared.parquet"
}

func normalizeDatasetProfile(profile *domain.DatasetProfile) *domain.DatasetProfile {
	if profile == nil {
		return nil
	}
	normalized := &domain.DatasetProfile{
		ProfileID:              strings.TrimSpace(profile.ProfileID),
		PreparePromptVersion:   trimStringPointer(profile.PreparePromptVersion),
		SentimentPromptVersion: trimStringPointer(profile.SentimentPromptVersion),
		RegexRuleNames:         normalizeStringList(profile.RegexRuleNames),
		GarbageRuleNames:       normalizeStringList(profile.GarbageRuleNames),
		EmbeddingModel:         trimStringPointer(profile.EmbeddingModel),
	}
	if normalized.ProfileID == "" &&
		normalized.PreparePromptVersion == nil &&
		normalized.SentimentPromptVersion == nil &&
		len(normalized.RegexRuleNames) == 0 &&
		len(normalized.GarbageRuleNames) == 0 &&
		normalized.EmbeddingModel == nil {
		return nil
	}
	return normalized
}

func cloneDatasetProfile(profile *domain.DatasetProfile) *domain.DatasetProfile {
	if profile == nil {
		return nil
	}
	cloned := &domain.DatasetProfile{
		ProfileID:        profile.ProfileID,
		RegexRuleNames:   append([]string(nil), profile.RegexRuleNames...),
		GarbageRuleNames: append([]string(nil), profile.GarbageRuleNames...),
	}
	if profile.PreparePromptVersion != nil {
		value := strings.TrimSpace(*profile.PreparePromptVersion)
		cloned.PreparePromptVersion = &value
	}
	if profile.SentimentPromptVersion != nil {
		value := strings.TrimSpace(*profile.SentimentPromptVersion)
		cloned.SentimentPromptVersion = &value
	}
	if profile.EmbeddingModel != nil {
		value := strings.TrimSpace(*profile.EmbeddingModel)
		cloned.EmbeddingModel = &value
	}
	return cloned
}

func normalizeStringList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func trimStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func datasetSourceForUnstructured(version domain.DatasetVersion) string {
	if isPrepareReady(version) && version.PrepareURI != nil && strings.TrimSpace(*version.PrepareURI) != "" {
		return strings.TrimSpace(*version.PrepareURI)
	}
	return strings.TrimSpace(version.StorageURI)
}

func datasetSourceForSentiment(version domain.DatasetVersion) string {
	if isSentimentReady(version) && version.SentimentURI != nil && strings.TrimSpace(*version.SentimentURI) != "" {
		return strings.TrimSpace(*version.SentimentURI)
	}
	return deriveSentimentURI(version)
}

func defaultPreparedTextColumn(version domain.DatasetVersion) string {
	if isPrepareReady(version) {
		return metadataString(version.Metadata, "prepared_text_column", metadataString(version.Metadata, "text_column", "normalized_text"))
	}
	return metadataString(version.Metadata, "text_column", "text")
}

func defaultPrepareRequired(dataType string, value *bool) bool {
	if value != nil {
		return *value
	}
	return dataType == "unstructured" || dataType == "mixed" || dataType == "both"
}

func requiresPrepare(version domain.DatasetVersion) bool {
	switch version.DataType {
	case "unstructured", "mixed", "both":
		return version.PrepareStatus != "not_requested" && version.PrepareStatus != "not_applicable"
	default:
		return false
	}
}

func isPrepareReady(version domain.DatasetVersion) bool {
	return version.PrepareStatus == "ready" && version.PrepareURI != nil && strings.TrimSpace(*version.PrepareURI) != ""
}

func isSentimentReady(version domain.DatasetVersion) bool {
	return version.SentimentStatus == "ready" && version.SentimentURI != nil && strings.TrimSpace(*version.SentimentURI) != ""
}

func embeddingBuildReady(version domain.DatasetVersion) bool {
	if version.EmbeddingStatus != "ready" {
		return false
	}
	if version.EmbeddingURI != nil && strings.TrimSpace(*version.EmbeddingURI) != "" {
		return true
	}
	if strings.TrimSpace(metadataString(version.Metadata, "embedding_index_source_ref", "")) != "" {
		return true
	}
	if strings.TrimSpace(metadataString(version.Metadata, "embedding_index_ref", "")) != "" {
		return true
	}
	return false
}

func artifactString(artifact map[string]any, key string) string {
	if artifact == nil {
		return ""
	}
	value, ok := artifact[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

func artifactMap(artifact map[string]any, key string) map[string]any {
	if artifact == nil {
		return nil
	}
	value, ok := artifact[key]
	if !ok || value == nil {
		return nil
	}
	typed, ok := value.(map[string]any)
	if !ok || len(typed) == 0 {
		return nil
	}
	return typed
}

func inferArtifactFormat(path string, fallback string) string {
	normalized := strings.ToLower(strings.TrimSpace(path))
	switch {
	case strings.HasSuffix(normalized, ".parquet"):
		return "parquet"
	case strings.HasSuffix(normalized, ".jsonl"):
		return "jsonl"
	default:
		return fallback
	}
}

func resolveReadableArtifactRef(candidates ...string) string {
	for _, candidate := range candidates {
		trimmed := strings.TrimSpace(candidate)
		if trimmed == "" {
			continue
		}
		if _, err := os.Stat(trimmed); err == nil {
			return trimmed
		}
	}
	for _, candidate := range candidates {
		trimmed := strings.TrimSpace(candidate)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func resolveReadableEmbeddingRef(primary string, fallback string, versionURI *string) string {
	return resolveReadableArtifactRef(primary, fallback, derefString(versionURI))
}

func (s *DatasetService) syncEmbeddingIndex(version domain.DatasetVersion, embeddingRef string, chunkRef string) error {
	indexer, ok := s.store.(store.EmbeddingChunkIndexer)
	if !ok {
		return nil
	}
	embeddingRef = strings.TrimSpace(embeddingRef)
	if embeddingRef == "" {
		return nil
	}
	records, err := loadEmbeddingIndexChunks(version.DatasetVersionID, embeddingRef, chunkRef, derefString(version.EmbeddingModel))
	if err != nil {
		return err
	}
	if err := indexer.ReplaceEmbeddingChunkIndex(version.DatasetVersionID, records); err != nil {
		return err
	}
	if version.Metadata == nil {
		version.Metadata = map[string]any{}
	}
	version.Metadata["embedding_index_backend"] = "pgvector"
	version.Metadata["embedding_index_ref"] = fmt.Sprintf("pgvector://embedding_index_chunks?dataset_version_id=%s", version.DatasetVersionID)
	vectorDim := 0
	if len(records) > 0 {
		vectorDim = records[0].VectorDim
	}
	version.Metadata["embedding_vector_dim"] = vectorDim
	version.Metadata["embedding_indexed_chunk_count"] = len(records)
	return nil
}

type embeddingSidecarRecord struct {
	SourceIndex       int64          `json:"source_index"`
	RowID             string         `json:"row_id"`
	ChunkID           string         `json:"chunk_id"`
	ChunkIndex        int            `json:"chunk_index"`
	CharStart         int            `json:"char_start"`
	CharEnd           int            `json:"char_end"`
	TokenCounts       map[string]int `json:"token_counts"`
	Embedding         []float32      `json:"embedding"`
	EmbeddingDim      int            `json:"embedding_dim"`
	EmbeddingProvider string         `json:"embedding_provider"`
}

type embeddingIndexParquetRecord struct {
	SourceIndex       int64
	RowID             string
	ChunkID           string
	ChunkIndex        int
	CharStart         int
	CharEnd           int
	EmbeddingJSON     string
	EmbeddingDim      int
	EmbeddingProvider string
	TokenCountsJSON   string
}

func loadEmbeddingIndexChunks(datasetVersionID string, embeddingRef string, chunkRef string, embeddingModel string) ([]domain.EmbeddingIndexChunk, error) {
	format := inferArtifactFormat(embeddingRef, "jsonl")
	if format == "parquet" {
		return loadEmbeddingIndexChunksFromParquet(datasetVersionID, embeddingRef, chunkRef, embeddingModel)
	}
	return loadEmbeddingIndexChunksFromJSONL(datasetVersionID, embeddingRef, chunkRef, embeddingModel)
}

func loadEmbeddingIndexChunksFromJSONL(datasetVersionID string, embeddingRef string, chunkRef string, embeddingModel string) ([]domain.EmbeddingIndexChunk, error) {
	handle, err := os.Open(embeddingRef)
	if err != nil {
		return nil, err
	}
	defer handle.Close()

	scanner := bufio.NewScanner(handle)
	scanner.Buffer(make([]byte, 1024), 4*1024*1024)
	records := make([]domain.EmbeddingIndexChunk, 0)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var sidecar embeddingSidecarRecord
		if err := json.Unmarshal([]byte(line), &sidecar); err != nil {
			return nil, err
		}
		if strings.TrimSpace(sidecar.ChunkID) == "" {
			continue
		}
		vector := sidecar.Embedding
		if len(vector) == 0 {
			vector = projectTokenCountsToDenseVector(sidecar.TokenCounts, tokenProjectionVectorDim)
		}
		vectorDim := len(vector)
		if vectorDim == 0 {
			continue
		}
		metadata := map[string]any{
			"char_start": sidecar.CharStart,
			"char_end":   sidecar.CharEnd,
		}
		if provider := strings.TrimSpace(sidecar.EmbeddingProvider); provider != "" {
			metadata["embedding_provider"] = provider
		}
		records = append(records, domain.EmbeddingIndexChunk{
			ChunkID:          strings.TrimSpace(sidecar.ChunkID),
			DatasetVersionID: datasetVersionID,
			RowID:            strings.TrimSpace(sidecar.RowID),
			SourceRowIndex:   sidecar.SourceIndex,
			ChunkIndex:       sidecar.ChunkIndex,
			ChunkRef:         strings.TrimSpace(chunkRef),
			EmbeddingModel:   strings.TrimSpace(embeddingModel),
			VectorDim:        vectorDim,
			Embedding:        vector,
			Metadata:         metadata,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func loadEmbeddingIndexChunksFromParquet(datasetVersionID string, embeddingRef string, chunkRef string, embeddingModel string) ([]domain.EmbeddingIndexChunk, error) {
	embeddingRef = strings.TrimSpace(embeddingRef)
	if embeddingRef == "" {
		return nil, errors.New("embedding parquet ref is required")
	}
	tempHandle, err := os.CreateTemp("", "embedding-index-*.duckdb")
	if err != nil {
		return nil, err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return nil, err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	defer os.Remove(dbPath)

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	query := fmt.Sprintf(
		`SELECT
			CAST(source_index AS BIGINT) AS source_index,
			COALESCE(row_id, '') AS row_id,
			COALESCE(chunk_id, '') AS chunk_id,
			CAST(chunk_index AS INTEGER) AS chunk_index,
			CAST(char_start AS INTEGER) AS char_start,
			CAST(char_end AS INTEGER) AS char_end,
			COALESCE(embedding_json, '') AS embedding_json,
			CAST(COALESCE(embedding_dim, 0) AS INTEGER) AS embedding_dim,
			COALESCE(embedding_provider, '') AS embedding_provider,
			COALESCE(token_counts_json, '{}') AS token_counts_json
		FROM read_parquet('%s')
		ORDER BY source_index, chunk_index, chunk_id`,
		escapeDuckDBLiteral(embeddingRef),
	)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make([]domain.EmbeddingIndexChunk, 0)
	for rows.Next() {
		var row embeddingIndexParquetRecord
		if err := rows.Scan(
			&row.SourceIndex,
			&row.RowID,
			&row.ChunkID,
			&row.ChunkIndex,
			&row.CharStart,
			&row.CharEnd,
			&row.EmbeddingJSON,
			&row.EmbeddingDim,
			&row.EmbeddingProvider,
			&row.TokenCountsJSON,
		); err != nil {
			return nil, err
		}
		record, ok, err := embeddingIndexChunkFromParquetRow(datasetVersionID, chunkRef, embeddingModel, row)
		if err != nil {
			return nil, err
		}
		if ok {
			records = append(records, record)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func embeddingIndexChunkFromParquetRow(datasetVersionID string, chunkRef string, embeddingModel string, row embeddingIndexParquetRecord) (domain.EmbeddingIndexChunk, bool, error) {
	if strings.TrimSpace(row.ChunkID) == "" {
		return domain.EmbeddingIndexChunk{}, false, nil
	}
	vector, err := parseFloat32JSONVector(row.EmbeddingJSON)
	if err != nil {
		return domain.EmbeddingIndexChunk{}, false, err
	}
	if len(vector) == 0 {
		tokenCounts, err := parseTokenCountsJSON(row.TokenCountsJSON)
		if err != nil {
			return domain.EmbeddingIndexChunk{}, false, err
		}
		vector = projectTokenCountsToDenseVector(tokenCounts, tokenProjectionVectorDim)
	}
	vectorDim := len(vector)
	if row.EmbeddingDim > 0 && len(vector) > 0 {
		vectorDim = row.EmbeddingDim
	}
	if vectorDim == 0 {
		return domain.EmbeddingIndexChunk{}, false, nil
	}
	metadata := map[string]any{
		"char_start": row.CharStart,
		"char_end":   row.CharEnd,
	}
	if provider := strings.TrimSpace(row.EmbeddingProvider); provider != "" {
		metadata["embedding_provider"] = provider
	}
	return domain.EmbeddingIndexChunk{
		ChunkID:          strings.TrimSpace(row.ChunkID),
		DatasetVersionID: datasetVersionID,
		RowID:            strings.TrimSpace(row.RowID),
		SourceRowIndex:   row.SourceIndex,
		ChunkIndex:       row.ChunkIndex,
		ChunkRef:         strings.TrimSpace(chunkRef),
		EmbeddingModel:   strings.TrimSpace(embeddingModel),
		VectorDim:        vectorDim,
		Embedding:        vector,
		Metadata:         metadata,
	}, true, nil
}

func parseFloat32JSONVector(value string) ([]float32, error) {
	text := strings.TrimSpace(value)
	if text == "" {
		return []float32{}, nil
	}
	var raw []float64
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil, err
	}
	vector := make([]float32, 0, len(raw))
	for _, item := range raw {
		vector = append(vector, float32(item))
	}
	return vector, nil
}

func parseTokenCountsJSON(value string) (map[string]int, error) {
	text := strings.TrimSpace(value)
	if text == "" {
		return map[string]int{}, nil
	}
	tokenCounts := map[string]int{}
	if err := json.Unmarshal([]byte(text), &tokenCounts); err != nil {
		return nil, err
	}
	return tokenCounts, nil
}

func escapeDuckDBLiteral(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "'", "''")
}

func projectTokenCountsToDenseVector(tokenCounts map[string]int, dim int) []float32 {
	if dim <= 0 {
		return []float32{}
	}
	vector := make([]float32, dim)
	for token, count := range tokenCounts {
		token = strings.TrimSpace(token)
		if token == "" || count == 0 {
			continue
		}
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(token))
		sum := hash.Sum64()
		index := int(sum % uint64(dim))
		sign := float32(1)
		if (sum>>63)&1 == 1 {
			sign = -1
		}
		vector[index] += sign * float32(count)
	}
	var norm float64
	for _, value := range vector {
		norm += float64(value * value)
	}
	if norm <= 0 {
		return vector
	}
	scale := float32(1 / math.Sqrt(norm))
	for index, value := range vector {
		vector[index] = value * scale
	}
	return vector
}

func anyToInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}

func mergeStringAny(base, overlay map[string]any) map[string]any {
	merged := map[string]any{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overlay {
		merged[key] = value
	}
	return merged
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(strings.TrimSpace(path))
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func sanitizeFilename(value string) string {
	trimmed := strings.TrimSpace(filepath.Base(value))
	if trimmed == "" || trimmed == "." || trimmed == string(filepath.Separator) {
		return ""
	}
	sanitized := strings.Map(func(r rune) rune {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			return r
		case r == '.', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, trimmed)
	sanitized = strings.Trim(sanitized, "._")
	if sanitized == "" {
		return ""
	}
	return sanitized
}
