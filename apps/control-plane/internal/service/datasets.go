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
	"analysis-support-platform/control-plane/internal/workflows"

	_ "github.com/marcboeker/go-duckdb"
)

const DefaultEmbeddingModel = "intfloat/multilingual-e5-small"
const tokenProjectionVectorDim = 64

const (
	workerTaskTimeoutPrepare               = 60 * time.Minute
	workerTaskTimeoutSentiment             = 30 * time.Minute
	workerTaskTimeoutEmbedding             = 45 * time.Minute
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
	defer resp.Body.Close()

	var decoded workerTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return workerTaskResponse{}, err
	}
	if resp.StatusCode >= 300 {
		return workerTaskResponse{}, workerTaskHTTPError{
			TaskPath:   taskPath,
			StatusCode: resp.StatusCode,
		}
	}
	return decoded, nil
}

type workerTaskHTTPError struct {
	TaskPath   string
	StatusCode int
}

func (e workerTaskHTTPError) Error() string {
	return fmt.Sprintf("worker task %s returned %d", e.TaskPath, e.StatusCode)
}

func workerTaskTimeout(taskPath string) time.Duration {
	switch strings.TrimSpace(taskPath) {
	case "/tasks/dataset_clean":
		return 20 * time.Minute
	case "/tasks/dataset_prepare":
		return workerTaskTimeoutPrepare
	case "/tasks/sentiment_label":
		return workerTaskTimeoutSentiment
	case "/tasks/embedding":
		return workerTaskTimeoutEmbedding
	case "/tasks/dataset_cluster_build":
		return workerTaskTimeoutEmbedding
	default:
		return 2 * time.Minute
	}
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

type datasetBuildTextSelection struct {
	TextColumn string
	Columns    []string
	Joiner     string
}

const defaultDatasetBuildTextJoiner = "\n\n"

func resolveDatasetBuildTextSelection(
	metadata map[string]any,
	inputColumns []string,
) datasetBuildTextSelection {
	columns := normalizeStringList(inputColumns)
	if len(columns) == 0 {
		columns = metadataStringList(metadata, "raw_text_columns")
	}
	if len(columns) == 0 {
		columns = metadataStringList(metadata, "text_columns")
	}
	return datasetBuildTextSelection{
		TextColumn: datasetBuildTextColumnLabel(columns),
		Columns:    append([]string(nil), columns...),
		Joiner:     defaultDatasetBuildTextJoiner,
	}
}

func resolveCleanPreprocessOptions(
	metadata map[string]any,
	input *domain.DatasetCleanPreprocessOptions,
) map[string]bool {
	if input != nil {
		return cleanPreprocessOptionsFromDomain(input)
	}
	if metadata == nil {
		return defaultCleanPreprocessOptions()
	}
	for _, key := range []string{"clean_preprocess_options", "preprocess_options"} {
		if options := cleanPreprocessOptionsFromAny(metadata[key]); len(options) > 0 {
			return options
		}
	}
	return defaultCleanPreprocessOptions()
}

func defaultCleanPreprocessOptions() map[string]bool {
	return map[string]bool{
		"remove_english":       false,
		"remove_numbers":       false,
		"remove_special":       false,
		"remove_monosyllables": false,
	}
}

func cleanPreprocessOptionsFromDomain(input *domain.DatasetCleanPreprocessOptions) map[string]bool {
	if input == nil {
		return defaultCleanPreprocessOptions()
	}
	return map[string]bool{
		"remove_english":       input.RemoveEnglish,
		"remove_numbers":       input.RemoveNumbers,
		"remove_special":       input.RemoveSpecial,
		"remove_monosyllables": input.RemoveMonosyllables,
	}
}

func cleanPreprocessOptionsFromAny(value any) map[string]bool {
	result := defaultCleanPreprocessOptions()
	hasKnownKey := false
	if source, ok := value.(map[string]bool); ok {
		for key := range result {
			if raw, exists := source[key]; exists {
				result[key] = raw
				hasKnownKey = true
			}
		}
		if !hasKnownKey {
			return nil
		}
		return result
	}
	source, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	for key := range result {
		if raw, exists := source[key]; exists {
			result[key] = anyBoolValue(raw)
			hasKnownKey = true
		}
	}
	if !hasKnownKey {
		return nil
	}
	return result
}

func hasEnabledPreparePreprocessOption(options map[string]bool) bool {
	for _, enabled := range options {
		if enabled {
			return true
		}
	}
	return false
}

func datasetBuildTextColumnLabel(columns []string) string {
	normalized := normalizeStringList(columns)
	if len(normalized) == 0 {
		return ""
	}
	if len(normalized) == 1 {
		return normalized[0]
	}
	return strings.Join(normalized, " + ")
}

func metadataRawString(metadata map[string]any, key string) (string, bool) {
	if metadata == nil {
		return "", false
	}
	value, ok := metadata[key]
	if !ok {
		return "", false
	}
	return anyStringValue(value), true
}

func metadataTime(metadata map[string]any, key string) (time.Time, bool) {
	if metadata == nil {
		return time.Time{}, false
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return time.Time{}, false
	}
	switch typed := value.(type) {
	case time.Time:
		return typed, true
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(typed))
		return parsed, err == nil
	default:
		return time.Time{}, false
	}
}

func metadataStringList(metadata map[string]any, key string) []string {
	if metadata == nil {
		return nil
	}
	value, ok := metadata[key]
	if !ok {
		return nil
	}
	return anyStringList(value)
}

func anyStringList(value any) []string {
	switch typed := value.(type) {
	case []string:
		return normalizeStringList(typed)
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			values = append(values, anyStringValue(item))
		}
		return normalizeStringList(values)
	case string:
		return normalizeStringList([]string{typed})
	default:
		return nil
	}
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

func shouldActivateDatasetVersionOnCreate(value *bool) bool {
	if value == nil {
		return true
	}
	return *value
}

func markDatasetVersionActive(version *domain.DatasetVersion, dataset domain.Dataset) {
	if version == nil {
		return
	}
	version.IsActive = dataset.ActiveDatasetVersionID != nil && *dataset.ActiveDatasetVersionID == version.DatasetVersionID
}

func (s *DatasetService) saveDatasetActiveVersion(dataset domain.Dataset, datasetVersionID *string) (domain.Dataset, error) {
	dataset.ActiveDatasetVersionID = trimStringPointer(datasetVersionID)
	now := time.Now().UTC()
	dataset.ActiveVersionUpdatedAt = &now
	if err := s.store.SaveDataset(dataset); err != nil {
		return domain.Dataset{}, err
	}
	return dataset, nil
}

func enrichDatasetVersionView(version *domain.DatasetVersion) {
	if version == nil {
		return
	}
	version.CleanStatus = cleanStatus(*version)
	if cleanedRef := metadataString(version.Metadata, "cleaned_ref", ""); cleanedRef != "" {
		version.CleanedRef = &cleanedRef
	}
	if cleanedAt, ok := metadataTime(version.Metadata, "cleaned_at"); ok {
		version.CleanedAt = &cleanedAt
	}
	version.CleanSummary = buildCleanSummary(version.Metadata)
	version.PrepareSummary = buildPrepareSummary(version.Metadata)
}

func buildCleanSummary(metadata map[string]any) *domain.DatasetCleanSummary {
	if len(metadata) == 0 {
		return nil
	}
	raw, ok := metadata["clean_summary"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	return &domain.DatasetCleanSummary{
		InputRowCount:         intValueOrZero(raw["input_row_count"]),
		OutputRowCount:        intValueOrZero(raw["output_row_count"]),
		KeptCount:             intValueOrZero(raw["kept_count"]),
		DroppedCount:          intValueOrZero(raw["dropped_count"]),
		SkippedRowCount:       intValueOrZero(raw["skipped_row_count"]),
		TextColumn:            strings.TrimSpace(anyStringValue(raw["text_column"])),
		TextColumns:           anyStringList(raw["text_columns"]),
		TextJoiner:            anyStringValue(raw["text_joiner"]),
		PreprocessOptions:     boolMapValue(raw["preprocess_options"]),
		SourceInputCharCount:  intValueOrZero(raw["source_input_char_count"]),
		CleanedInputCharCount: intValueOrZero(raw["cleaned_input_char_count"]),
		CleanReducedCharCount: intValueOrZero(raw["clean_reduced_char_count"]),
		CleanRegexRuleHits:    intMapValue(raw["clean_regex_rule_hits"]),
	}
}

func buildSentimentSummary(metadata map[string]any) *domain.DatasetSentimentSummary {
	if len(metadata) == 0 {
		return nil
	}
	raw, ok := metadata["sentiment_summary"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	return &domain.DatasetSentimentSummary{
		InputRowCount:      intValueOrZero(raw["input_row_count"]),
		LabeledRowCount:    intValueOrZero(raw["labeled_row_count"]),
		TextColumn:         strings.TrimSpace(anyStringValue(raw["text_column"])),
		TextColumns:        anyStringList(raw["text_columns"]),
		TextJoiner:         anyStringValue(raw["text_joiner"]),
		SentimentBatchSize: intValueOrZero(raw["sentiment_batch_size"]),
		LabelCounts:        intMapValue(raw["label_counts"]),
	}
}

func buildPrepareSummary(metadata map[string]any) *domain.DatasetPrepareSummary {
	if len(metadata) == 0 {
		return nil
	}
	raw, ok := metadata["prepare_summary"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	return &domain.DatasetPrepareSummary{
		InputRowCount:  intValueOrZero(raw["input_row_count"]),
		OutputRowCount: intValueOrZero(raw["output_row_count"]),
		KeptCount:      intValueOrZero(raw["kept_count"]),
		ReviewCount:    intValueOrZero(raw["review_count"]),
		DroppedCount:   intValueOrZero(raw["dropped_count"]),
		TextColumn:     strings.TrimSpace(anyStringValue(raw["text_column"])),
		TextColumns:    anyStringList(raw["text_columns"]),
		TextJoiner:     anyStringValue(raw["text_joiner"]),
	}
}

func cloneSentimentSummary(summary *domain.DatasetSentimentSummary) *domain.DatasetSentimentSummary {
	if summary == nil {
		return nil
	}
	cloned := *summary
	if len(summary.TextColumns) > 0 {
		cloned.TextColumns = append([]string(nil), summary.TextColumns...)
	}
	if len(summary.LabelCounts) > 0 {
		cloned.LabelCounts = cloneStringIntMap(summary.LabelCounts)
	}
	return &cloned
}

func clonePrepareSummary(summary *domain.DatasetPrepareSummary) *domain.DatasetPrepareSummary {
	if summary == nil {
		return nil
	}
	cloned := *summary
	if len(summary.TextColumns) > 0 {
		cloned.TextColumns = append([]string(nil), summary.TextColumns...)
	}
	return &cloned
}

func datasetSourceForUnstructured(version domain.DatasetVersion) string {
	return domain.ResolveDatasetSource(version).DatasetName
}

func datasetSourceForPrepare(version domain.DatasetVersion) (string, []string) {
	source := domain.ResolvePrepareInputSource(version)
	return source.DatasetName, source.TextColumns
}

func datasetSourceForSentiment(version domain.DatasetVersion) string {
	if isSentimentReady(version) && version.SentimentURI != nil && strings.TrimSpace(*version.SentimentURI) != "" {
		return strings.TrimSpace(*version.SentimentURI)
	}
	return deriveSentimentURI(version)
}

func defaultPreparedTextColumn(version domain.DatasetVersion) string {
	return domain.DatasetSourceDefaultTextColumn(version)
}

func textSelectionMatchesRawSource(version domain.DatasetVersion, textColumn string, textColumns []string) bool {
	if domain.DatasetSourceIsRawTextColumn(version, textColumn) {
		return true
	}
	for _, column := range textColumns {
		if domain.DatasetSourceIsRawTextColumn(version, column) {
			return true
		}
	}
	return false
}

func defaultPrepareRequired(dataType string, value *bool) bool {
	if value != nil {
		return *value
	}
	return false
}

func requiresClean(version domain.DatasetVersion) bool {
	switch version.DataType {
	case "unstructured", "mixed", "both":
		return cleanStatus(version) != "not_applicable"
	default:
		return false
	}
}

func requiresPrepare(version domain.DatasetVersion) bool {
	switch version.DataType {
	case "unstructured", "mixed", "both":
		return version.PrepareStatus != "not_requested" && version.PrepareStatus != "not_applicable"
	default:
		return false
	}
}

func requiresSentiment(version domain.DatasetVersion) bool {
	switch version.DataType {
	case "unstructured", "mixed", "both":
		return version.SentimentStatus != "" && version.SentimentStatus != "not_requested" && version.SentimentStatus != "not_applicable"
	default:
		return false
	}
}

func cleanStatus(version domain.DatasetVersion) string {
	status := strings.TrimSpace(metadataString(version.Metadata, "clean_status", ""))
	if status != "" {
		return status
	}
	switch version.DataType {
	case "unstructured", "mixed", "both":
		return "not_requested"
	default:
		return "not_applicable"
	}
}

func isCleanReady(version domain.DatasetVersion) bool {
	return cleanStatus(version) == "ready" && strings.TrimSpace(metadataString(version.Metadata, "cleaned_ref", "")) != ""
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

func datasetClusterReady(version domain.DatasetVersion) bool {
	if strings.TrimSpace(metadataString(version.Metadata, "cluster_status", "")) != "ready" {
		return false
	}
	return strings.TrimSpace(metadataString(version.Metadata, "cluster_ref", "")) != ""
}

func resolvePrepareArtifact(version domain.DatasetVersion) (string, string, error) {
	preparedRef := strings.TrimSpace(metadataString(version.Metadata, "prepared_ref", ""))
	if preparedRef == "" && version.PrepareURI != nil {
		preparedRef = strings.TrimSpace(*version.PrepareURI)
	}
	if version.PrepareStatus != "ready" || preparedRef == "" {
		return "", "", ErrInvalidArgument{Message: "prepare artifact is not ready"}
	}
	prepareFormat := strings.TrimSpace(metadataString(version.Metadata, "prepared_format", ""))
	if prepareFormat == "" {
		prepareFormat = strings.TrimSpace(metadataString(version.Metadata, "prepare_format", ""))
	}
	if prepareFormat == "" {
		prepareFormat = inferArtifactFormat(preparedRef, "parquet")
	}
	return preparedRef, prepareFormat, nil
}

func resolveSentimentArtifact(version domain.DatasetVersion) (string, string, error) {
	sentimentRef := strings.TrimSpace(metadataString(version.Metadata, "sentiment_ref", ""))
	if sentimentRef == "" && version.SentimentURI != nil {
		sentimentRef = strings.TrimSpace(*version.SentimentURI)
	}
	if version.SentimentStatus != "ready" || sentimentRef == "" {
		return "", "", ErrInvalidArgument{Message: "sentiment artifact is not ready"}
	}
	sentimentFormat := strings.TrimSpace(metadataString(version.Metadata, "sentiment_format", ""))
	if sentimentFormat == "" {
		sentimentFormat = inferArtifactFormat(sentimentRef, "parquet")
	}
	return sentimentRef, sentimentFormat, nil
}

func deriveClusterMembershipURI(summaryURI string) string {
	summaryURI = strings.TrimSpace(summaryURI)
	if summaryURI == "" {
		return ""
	}
	if strings.HasSuffix(summaryURI, ".json") {
		return strings.TrimSuffix(summaryURI, ".json") + ".memberships.parquet"
	}
	return summaryURI + ".memberships.parquet"
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

func artifactBoolMap(artifact map[string]any, key string) map[string]bool {
	return boolMapValue(artifactMap(artifact, key))
}

type llmFallbackInfo struct {
	Fallback bool
	Reason   string
	Count    int
	Provider string
	Model    string
}

func clearLLMFallbackMetadata(metadata map[string]any, prefix string) {
	if metadata == nil {
		return
	}
	delete(metadata, prefix+"_llm_fallback")
	delete(metadata, prefix+"_llm_fallback_reason")
	delete(metadata, prefix+"_llm_fallback_count")
	delete(metadata, prefix+"_llm_fallback_reasons")
	delete(metadata, prefix+"_llm_provider")
	delete(metadata, prefix+"_llm_model")
	delete(metadata, prefix+"_warning")
}

func applyLLMFallbackMetadata(metadata map[string]any, prefix string, artifact map[string]any) llmFallbackInfo {
	info := llmFallbackInfo{
		Fallback: artifactBool(artifact, "llm_fallback"),
		Reason:   artifactString(artifact, "llm_fallback_reason"),
		Provider: artifactString(artifact, "llm_provider"),
		Model:    artifactString(artifact, "llm_model"),
	}
	if count, ok := artifactInt(artifact, "llm_fallback_count"); ok {
		info.Count = count
	}
	if !info.Fallback {
		return info
	}
	metadata[prefix+"_llm_fallback"] = true
	metadata[prefix+"_llm_fallback_count"] = info.Count
	if info.Reason != "" {
		metadata[prefix+"_llm_fallback_reason"] = info.Reason
		metadata[prefix+"_warning"] = fmt.Sprintf("%s llm fallback used: %s", prefix, info.Reason)
	} else {
		metadata[prefix+"_warning"] = fmt.Sprintf("%s llm fallback used", prefix)
	}
	if reasons := metadataStringList(artifact, "llm_fallback_reasons"); len(reasons) > 0 {
		metadata[prefix+"_llm_fallback_reasons"] = reasons
	}
	if info.Provider != "" {
		metadata[prefix+"_llm_provider"] = info.Provider
	}
	if info.Model != "" {
		metadata[prefix+"_llm_model"] = info.Model
	}
	return info
}

func artifactBool(artifact map[string]any, key string) bool {
	if artifact == nil {
		return false
	}
	value, ok := artifact[key]
	if !ok || value == nil {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func artifactInt(artifact map[string]any, key string) (int, bool) {
	if artifact == nil {
		return 0, false
	}
	value, ok := artifact[key]
	if !ok || value == nil {
		return 0, false
	}
	return anyToInt(value)
}

func normalizeOptionalPositiveInt(value *int, fieldName string) (int, error) {
	if value == nil {
		return 0, nil
	}
	normalized := *value
	if normalized <= 0 {
		return 0, ErrInvalidArgument{Message: fmt.Sprintf("%s must be a positive integer", fieldName)}
	}
	return normalized, nil
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

type preparePreviewParquetRecord struct {
	SourceRowIndex     int
	RowID              string
	RawText            string
	NormalizedText     string
	PrepareDisposition string
	PrepareReason      string
}

type sentimentPreviewParquetRecord struct {
	SourceRowIndex      int
	RowID               string
	SentimentLabel      string
	SentimentConfidence float64
	SentimentReason     string
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

type clusterMembershipParquetRecord struct {
	ClusterID            string
	ClusterRank          int
	ClusterDocumentCount int
	SourceIndex          int
	RowID                string
	ChunkID              string
	ChunkIndex           int
	Text                 string
	IsSample             bool
}

func loadClusterSummary(summaryRef string, clusterID string) (map[string]any, error) {
	summaryRef = strings.TrimSpace(summaryRef)
	if summaryRef == "" {
		return nil, ErrInvalidArgument{Message: "cluster summary ref is required"}
	}
	content, err := os.ReadFile(summaryRef)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound{Resource: "cluster summary artifact"}
		}
		return nil, err
	}
	var artifact map[string]any
	if err := json.Unmarshal(content, &artifact); err != nil {
		return nil, err
	}
	clusters, ok := artifact["clusters"].([]any)
	if !ok {
		return nil, ErrInvalidArgument{Message: "cluster summary artifact is invalid"}
	}
	for _, raw := range clusters {
		cluster, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if strings.TrimSpace(anyStringValue(cluster["cluster_id"])) != clusterID {
			continue
		}
		return mergeStringAny(nil, cluster), nil
	}
	return nil, ErrNotFound{Resource: "cluster"}
}

func loadDatasetSourceSummary(storageURI string, sampleLimit int) *domain.DatasetSourceSummary {
	storageURI = strings.TrimSpace(storageURI)
	summary := &domain.DatasetSourceSummary{
		Available:   false,
		Status:      "unavailable",
		SampleLimit: sampleLimit,
	}
	if storageURI == "" {
		summary.Status = "missing"
		summary.ErrorMessage = "storage_uri is required"
		return summary
	}

	format := inferDatasetSourceFormat(storageURI)
	if format == "" {
		summary.Status = "unsupported"
		summary.ErrorMessage = "unsupported source format"
		return summary
	}
	summary.Format = format

	info, err := os.Stat(storageURI)
	if err != nil {
		if os.IsNotExist(err) {
			summary.Status = "missing"
			summary.ErrorMessage = "source file not found"
			return summary
		}
		summary.Status = "error"
		summary.ErrorMessage = err.Error()
		return summary
	}
	if info.IsDir() {
		summary.Status = "error"
		summary.ErrorMessage = "source path must be a file"
		return summary
	}

	relation, err := datasetSourceDuckDBRelation(storageURI, format)
	if err != nil {
		summary.Status = "unsupported"
		summary.ErrorMessage = err.Error()
		return summary
	}
	db, cleanup, err := openTemporaryDuckDB("dataset-source-summary-*.duckdb")
	if err != nil {
		summary.Status = "error"
		summary.ErrorMessage = err.Error()
		return summary
	}
	defer cleanup()

	columns, err := loadDatasetSourceColumns(db, relation)
	if err != nil {
		summary.Status = "error"
		summary.ErrorMessage = err.Error()
		return summary
	}
	rowCount, err := loadDatasetSourceRowCount(db, relation)
	if err != nil {
		summary.Status = "error"
		summary.ErrorMessage = err.Error()
		return summary
	}
	sampleRows, err := loadDatasetSourceSampleRows(db, relation, sampleLimit)
	if err != nil {
		summary.Status = "error"
		summary.ErrorMessage = err.Error()
		return summary
	}

	summary.Available = true
	summary.Status = "ready"
	summary.RowCount = &rowCount
	summary.ColumnCount = len(columns)
	summary.Columns = columns
	summary.SampleRows = sampleRows
	summary.ErrorMessage = ""
	return summary
}

func inferDatasetSourceFormat(path string) string {
	normalized := strings.ToLower(strings.TrimSpace(path))
	switch {
	case strings.HasSuffix(normalized, ".parquet"):
		return "parquet"
	case strings.HasSuffix(normalized, ".csv"):
		return "csv"
	case strings.HasSuffix(normalized, ".tsv"):
		return "tsv"
	case strings.HasSuffix(normalized, ".jsonl"), strings.HasSuffix(normalized, ".ndjson"):
		return "jsonl"
	default:
		return ""
	}
}

func datasetSourceDuckDBRelation(path string, format string) (string, error) {
	escapedPath := escapeDuckDBLiteral(path)
	switch format {
	case "parquet":
		return fmt.Sprintf("read_parquet('%s')", escapedPath), nil
	case "csv", "tsv":
		return fmt.Sprintf("read_csv_auto('%s', HEADER=TRUE, SAMPLE_SIZE=-1)", escapedPath), nil
	case "jsonl":
		return fmt.Sprintf("read_json_auto('%s')", escapedPath), nil
	default:
		return "", ErrInvalidArgument{Message: "unsupported source format"}
	}
}

func openTemporaryDuckDB(pattern string) (*sql.DB, func(), error) {
	tempHandle, err := os.CreateTemp("", pattern)
	if err != nil {
		return nil, nil, err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return nil, nil, err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return nil, nil, err
	}
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		_ = os.Remove(dbPath)
		return nil, nil, err
	}
	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(dbPath)
	}
	return db, cleanup, nil
}

func loadDatasetSourceColumns(db *sql.DB, relation string) ([]domain.DatasetSourceColumnSummary, error) {
	rows, err := db.Query(fmt.Sprintf("DESCRIBE SELECT * FROM %s", relation))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make([]domain.DatasetSourceColumnSummary, 0)
	for rows.Next() {
		var name sql.NullString
		var columnType sql.NullString
		var nullable sql.NullString
		var key sql.NullString
		var defaultValue sql.NullString
		var extra sql.NullString
		if err := rows.Scan(&name, &columnType, &nullable, &key, &defaultValue, &extra); err != nil {
			return nil, err
		}
		columnName := strings.TrimSpace(name.String)
		if columnName == "" {
			continue
		}
		columns = append(columns, domain.DatasetSourceColumnSummary{
			Name: columnName,
			Type: strings.TrimSpace(columnType.String),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func loadDatasetSourceRowCount(db *sql.DB, relation string) (int, error) {
	var rowCount int64
	query := fmt.Sprintf("SELECT CAST(COUNT(*) AS BIGINT) FROM %s", relation)
	if err := db.QueryRow(query).Scan(&rowCount); err != nil {
		return 0, err
	}
	return int(rowCount), nil
}

func loadDatasetSourceSampleRows(db *sql.DB, relation string, limit int) ([]map[string]any, error) {
	if limit <= 0 {
		return nil, nil
	}
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT %d", relation, limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0)
	for rows.Next() {
		values := make([]any, len(columnNames))
		destinations := make([]any, len(columnNames))
		for index := range values {
			destinations[index] = &values[index]
		}
		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}
		item := make(map[string]any, len(columnNames))
		for index, column := range columnNames {
			item[column] = sourceSummaryJSONValue(values[index])
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func sourceSummaryJSONValue(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case []byte:
		return string(typed)
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	default:
		return typed
	}
}

func loadPrepareSamplesFromParquet(preparedRef string, limit int, disposition string) ([]domain.DatasetPrepareSample, error) {
	preparedRef = strings.TrimSpace(preparedRef)
	if preparedRef == "" {
		return nil, ErrInvalidArgument{Message: "prepare artifact ref is required"}
	}
	if limit <= 0 {
		return nil, ErrInvalidArgument{Message: "limit must be a positive integer"}
	}
	if _, err := os.Stat(preparedRef); err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound{Resource: "prepare artifact"}
		}
		return nil, err
	}
	tempHandle, err := os.CreateTemp("", "prepare-preview-*.duckdb")
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

	filterClause := ""
	disposition = strings.TrimSpace(disposition)
	if disposition != "" {
		filterClause = fmt.Sprintf("WHERE prepare_disposition = '%s'", escapeDuckDBLiteral(disposition))
	}
	query := fmt.Sprintf(
		`SELECT
			CAST(COALESCE(source_row_index, 0) AS INTEGER) AS source_row_index,
			COALESCE(row_id, '') AS row_id,
			COALESCE(raw_text, '') AS raw_text,
			COALESCE(normalized_text, '') AS normalized_text,
			COALESCE(prepare_disposition, '') AS prepare_disposition,
			COALESCE(prepare_reason, '') AS prepare_reason
		FROM read_parquet('%s')
		%s
		ORDER BY source_row_index, row_id
		LIMIT %d`,
		escapeDuckDBLiteral(preparedRef),
		filterClause,
		limit,
	)
	rows, err := db.Query(query)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no files found") {
			return nil, ErrNotFound{Resource: "prepare artifact"}
		}
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.DatasetPrepareSample, 0)
	for rows.Next() {
		var row preparePreviewParquetRecord
		if err := rows.Scan(
			&row.SourceRowIndex,
			&row.RowID,
			&row.RawText,
			&row.NormalizedText,
			&row.PrepareDisposition,
			&row.PrepareReason,
		); err != nil {
			return nil, err
		}
		items = append(items, domain.DatasetPrepareSample{
			SourceRowIndex:     row.SourceRowIndex,
			RowID:              strings.TrimSpace(row.RowID),
			RawText:            strings.TrimSpace(row.RawText),
			NormalizedText:     strings.TrimSpace(row.NormalizedText),
			PrepareDisposition: strings.TrimSpace(row.PrepareDisposition),
			PrepareReason:      strings.TrimSpace(row.PrepareReason),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func loadSentimentSamplesFromParquet(sentimentRef string, limit int) ([]domain.DatasetSentimentSample, error) {
	sentimentRef = strings.TrimSpace(sentimentRef)
	if sentimentRef == "" {
		return nil, ErrInvalidArgument{Message: "sentiment artifact ref is required"}
	}
	if limit <= 0 {
		return nil, ErrInvalidArgument{Message: "limit must be a positive integer"}
	}
	if _, err := os.Stat(sentimentRef); err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound{Resource: "sentiment artifact"}
		}
		return nil, err
	}
	tempHandle, err := os.CreateTemp("", "sentiment-preview-*.duckdb")
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
			CAST(COALESCE(source_row_index, 0) AS INTEGER) AS source_row_index,
			COALESCE(row_id, '') AS row_id,
			COALESCE(sentiment_label, '') AS sentiment_label,
			CAST(COALESCE(sentiment_confidence, 0) AS DOUBLE) AS sentiment_confidence,
			COALESCE(sentiment_reason, '') AS sentiment_reason
		FROM read_parquet('%s')
		ORDER BY source_row_index, row_id
		LIMIT %d`,
		escapeDuckDBLiteral(sentimentRef),
		limit,
	)
	rows, err := db.Query(query)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no files found") {
			return nil, ErrNotFound{Resource: "sentiment artifact"}
		}
		return nil, err
	}
	defer rows.Close()

	items := make([]domain.DatasetSentimentSample, 0)
	for rows.Next() {
		var row sentimentPreviewParquetRecord
		if err := rows.Scan(
			&row.SourceRowIndex,
			&row.RowID,
			&row.SentimentLabel,
			&row.SentimentConfidence,
			&row.SentimentReason,
		); err != nil {
			return nil, err
		}
		items = append(items, domain.DatasetSentimentSample{
			SourceRowIndex:      row.SourceRowIndex,
			RowID:               strings.TrimSpace(row.RowID),
			SentimentLabel:      strings.TrimSpace(row.SentimentLabel),
			SentimentConfidence: row.SentimentConfidence,
			SentimentReason:     strings.TrimSpace(row.SentimentReason),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func exportPrepareCSVFromParquet(preparedRef string) (string, error) {
	preparedRef = strings.TrimSpace(preparedRef)
	if preparedRef == "" {
		return "", ErrInvalidArgument{Message: "prepare artifact ref is required"}
	}
	if _, err := os.Stat(preparedRef); err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound{Resource: "prepare artifact"}
		}
		return "", err
	}

	csvHandle, err := os.CreateTemp("", "prepare-export-*.csv")
	if err != nil {
		return "", err
	}
	csvPath := csvHandle.Name()
	if err := csvHandle.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(csvPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}

	tempHandle, err := os.CreateTemp("", "prepare-export-*.duckdb")
	if err != nil {
		return "", err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	defer os.Remove(dbPath)

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return "", err
	}
	defer db.Close()

	query := fmt.Sprintf(
		`COPY (
			SELECT * FROM read_parquet('%s')
			ORDER BY source_row_index, row_id
		) TO '%s' (FORMAT CSV, HEADER)`,
		escapeDuckDBLiteral(preparedRef),
		escapeDuckDBLiteral(csvPath),
	)
	if _, err := db.Exec(query); err != nil {
		_ = os.Remove(csvPath)
		return "", err
	}
	return csvPath, nil
}

func exportSentimentCSVFromParquet(sentimentRef string) (string, error) {
	sentimentRef = strings.TrimSpace(sentimentRef)
	if sentimentRef == "" {
		return "", ErrInvalidArgument{Message: "sentiment artifact ref is required"}
	}
	if _, err := os.Stat(sentimentRef); err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound{Resource: "sentiment artifact"}
		}
		return "", err
	}

	csvHandle, err := os.CreateTemp("", "sentiment-export-*.csv")
	if err != nil {
		return "", err
	}
	csvPath := csvHandle.Name()
	if err := csvHandle.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(csvPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}

	tempHandle, err := os.CreateTemp("", "sentiment-export-*.duckdb")
	if err != nil {
		return "", err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	defer os.Remove(dbPath)

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return "", err
	}
	defer db.Close()

	query := fmt.Sprintf(
		`COPY (
			SELECT * FROM read_parquet('%s')
			ORDER BY source_row_index, row_id
		) TO '%s' (FORMAT CSV, HEADER)`,
		escapeDuckDBLiteral(sentimentRef),
		escapeDuckDBLiteral(csvPath),
	)
	if _, err := db.Exec(query); err != nil {
		_ = os.Remove(csvPath)
		return "", err
	}
	return csvPath, nil
}

func loadClusterMembersFromParquet(clusterMembershipRef string, clusterID string, limit int, samplesOnly bool) ([]domain.ClusterMember, int, int, error) {
	clusterMembershipRef = strings.TrimSpace(clusterMembershipRef)
	if clusterMembershipRef == "" {
		return nil, 0, 0, ErrInvalidArgument{Message: "cluster membership ref is required"}
	}
	tempHandle, err := os.CreateTemp("", "cluster-members-*.duckdb")
	if err != nil {
		return nil, 0, 0, err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return nil, 0, 0, err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return nil, 0, 0, err
	}
	defer os.Remove(dbPath)

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, 0, 0, err
	}
	defer db.Close()

	countQuery := fmt.Sprintf(
		`SELECT
			CAST(COUNT(*) AS BIGINT) AS total_count,
			CAST(COALESCE(SUM(CASE WHEN is_sample THEN 1 ELSE 0 END), 0) AS BIGINT) AS sample_count
		FROM read_parquet('%s')
		WHERE cluster_id = '%s'`,
		escapeDuckDBLiteral(clusterMembershipRef),
		escapeDuckDBLiteral(clusterID),
	)
	var totalCount int
	var sampleCount int
	if err := db.QueryRow(countQuery).Scan(&totalCount, &sampleCount); err != nil {
		return nil, 0, 0, err
	}

	filterClause := ""
	if samplesOnly {
		filterClause = " AND is_sample = TRUE"
	}
	rowsQuery := fmt.Sprintf(
		`SELECT
			COALESCE(cluster_id, '') AS cluster_id,
			CAST(COALESCE(cluster_rank, 0) AS INTEGER) AS cluster_rank,
			CAST(COALESCE(cluster_document_count, 0) AS INTEGER) AS cluster_document_count,
			CAST(COALESCE(source_index, 0) AS INTEGER) AS source_index,
			COALESCE(row_id, '') AS row_id,
			COALESCE(chunk_id, '') AS chunk_id,
			CAST(COALESCE(chunk_index, 0) AS INTEGER) AS chunk_index,
			COALESCE(text, '') AS text,
			CAST(COALESCE(is_sample, FALSE) AS BOOLEAN) AS is_sample
		FROM read_parquet('%s')
		WHERE cluster_id = '%s'%s
		ORDER BY is_sample DESC, source_index, chunk_index, chunk_id
		LIMIT %d`,
		escapeDuckDBLiteral(clusterMembershipRef),
		escapeDuckDBLiteral(clusterID),
		filterClause,
		limit,
	)
	rows, err := db.Query(rowsQuery)
	if err != nil {
		return nil, 0, 0, err
	}
	defer rows.Close()

	items := make([]domain.ClusterMember, 0)
	for rows.Next() {
		var row clusterMembershipParquetRecord
		if err := rows.Scan(
			&row.ClusterID,
			&row.ClusterRank,
			&row.ClusterDocumentCount,
			&row.SourceIndex,
			&row.RowID,
			&row.ChunkID,
			&row.ChunkIndex,
			&row.Text,
			&row.IsSample,
		); err != nil {
			return nil, 0, 0, err
		}
		items = append(items, domain.ClusterMember{
			ClusterID:            strings.TrimSpace(row.ClusterID),
			ClusterRank:          row.ClusterRank,
			ClusterDocumentCount: row.ClusterDocumentCount,
			SourceIndex:          row.SourceIndex,
			RowID:                strings.TrimSpace(row.RowID),
			ChunkID:              strings.TrimSpace(row.ChunkID),
			ChunkIndex:           row.ChunkIndex,
			Text:                 strings.TrimSpace(row.Text),
			IsSample:             row.IsSample,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, 0, err
	}
	return items, totalCount, sampleCount, nil
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

func intValueOrZero(value any) int {
	if converted, ok := anyToInt(value); ok {
		return converted
	}
	return 0
}

func intMapValue(value any) map[string]int {
	source, ok := value.(map[string]any)
	if !ok || len(source) == 0 {
		return nil
	}
	result := make(map[string]int, len(source))
	for key, item := range source {
		if count, ok := anyToInt(item); ok {
			result[key] = count
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func boolMapValue(value any) map[string]bool {
	if source, ok := value.(map[string]bool); ok {
		return cloneStringBoolMap(source)
	}
	source, ok := value.(map[string]any)
	if !ok || len(source) == 0 {
		return nil
	}
	result := make(map[string]bool, len(source))
	for key, item := range source {
		result[key] = anyBoolValue(item)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func anyBoolValue(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		normalized := strings.TrimSpace(strings.ToLower(typed))
		return normalized == "true" || normalized == "1" || normalized == "yes" || normalized == "y"
	case int:
		return typed != 0
	case int32:
		return typed != 0
	case int64:
		return typed != 0
	case float64:
		return typed != 0
	default:
		return false
	}
}

func cloneStringIntMap(source map[string]int) map[string]int {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]int, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func cloneStringBoolMap(source map[string]bool) map[string]bool {
	if len(source) == 0 {
		return nil
	}
	result := make(map[string]bool, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
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

func metadataNestedString(metadata map[string]any, key, field string) string {
	if metadata == nil {
		return ""
	}
	nested, ok := metadata[key].(map[string]any)
	if !ok {
		return ""
	}
	value, ok := nested[field]
	if !ok {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
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
