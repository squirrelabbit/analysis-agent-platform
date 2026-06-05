package service

import (
	"fmt"
	"io"
	"sort"
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

	// silverone 2026-05-28 (β2 cleanup PR2) — struct 필드 제거 후 prepare /
	// sentiment / embedding 관련 status는 metadata jsonb로만 보존.
	if prepareStatus != "" {
		metadata["prepare_status"] = prepareStatus
	}
	if prepareLLMMode != "" {
		metadata["prepare_llm_mode"] = prepareLLMMode
	}
	if input.PrepareModel != nil && strings.TrimSpace(*input.PrepareModel) != "" {
		metadata["prepare_model"] = *input.PrepareModel
	}
	if sentimentStatus != "" {
		metadata["sentiment_status"] = sentimentStatus
	}
	if sentimentLLMMode != "" {
		metadata["sentiment_llm_mode"] = sentimentLLMMode
	}
	if input.SentimentModel != nil && strings.TrimSpace(*input.SentimentModel) != "" {
		metadata["sentiment_model"] = *input.SentimentModel
	}
	if embeddingStatus != "" {
		metadata["embedding_status"] = embeddingStatus
	}
	if input.EmbeddingModel != nil && strings.TrimSpace(*input.EmbeddingModel) != "" {
		metadata["embedding_model"] = *input.EmbeddingModel
	}

	// silverone 2026-06-04 — 생성 시점에 display용 version_number를 부여한다.
	// 같은 dataset의 기존 버전 최대 번호 + 1 (생성순 1-based). metadata에 저장해
	// 삭제가 일어나도 남은 버전 번호를 재계산하지 않는다(stable). 동일 파일명을
	// 여러 번 올려도 번호로 구분 가능. read 시 fallback 규칙은 numberDatasetVersions 참조.
	existingVersions, err := s.store.ListDatasetVersions(projectID, dataset.DatasetID)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	metadata[versionNumberMetaKey] = nextVersionNumber(existingVersions)

	version := domain.DatasetVersion{
		DatasetVersionID: id.New(),
		DatasetID:        dataset.DatasetID,
		ProjectID:        projectID,
		StorageURI:       storageURI,
		DataType:         dataType,
		RecordCount:      input.RecordCount,
		Metadata:         metadata,
		Profile:          profile,
		CreatedAt:        time.Now().UTC(),
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
		version.CleanStatus = strings.TrimSpace(fmt.Sprintf("%v", version.Metadata["clean_status"]))
		if version.CleanStatus == "" {
			version.CleanStatus = "not_requested"
		}
	} else {
		version.CleanStatus = "not_applicable"
	}
	if sentimentRequired {
		version.Metadata["sentiment_required"] = true
	}
	if embeddingRequired {
		version.Metadata["embedding_required"] = true
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
	if _, err := s.CreateCleanJob(projectID, datasetID, version.DatasetVersionID, domain.DatasetCleanRequest{}, "dataset_version_create", ""); err == nil {
		latest, getErr := s.GetDatasetVersion(projectID, datasetID, version.DatasetVersionID)
		if getErr == nil {
			return latest
		}
	}
	return version
}

// maybeRunEagerSentiment / maybeRunEagerEmbedding / maybeRunEagerPostPrepareBuilds
// 제거됨 — dataset_build 7 task 제거에 따라 prepare 후 자동 sentiment/embedding
// trigger도 사라졌다. DatasetVersion struct field 정리는 후속 task.

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
	if err := s.attachDatasetVersionArtifacts(&version); err != nil {
		return domain.DatasetVersion{}, err
	}
	buildJobs, err := s.latestDatasetVersionBuildJobStatuses(projectID, version)
	if err != nil {
		return domain.DatasetVersion{}, err
	}
	version.BuildJobs = buildJobs
	version.BuildStages = buildDatasetVersionStages(version, buildJobs)
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
	numByID := numberDatasetVersions(items)
	list := make([]domain.DatasetVersionListItem, 0, len(items))
	for index := range items {
		enrichDatasetVersionView(&items[index])
		markDatasetVersionActive(&items[index], dataset)
		item := summarizeDatasetVersionForList(items[index])
		item.VersionNumber = numByID[items[index].DatasetVersionID]
		list = append(list, item)
	}
	return domain.DatasetVersionListResponse{Items: list}, nil
}

const versionNumberMetaKey = "version_number"

// storedVersionNumber — 생성 시점에 metadata에 저장한 version_number(없으면 0).
// JSONB는 숫자를 float64로 역직렬화하므로 int/int64/float64 모두 처리.
func storedVersionNumber(meta map[string]any) int {
	if meta == nil {
		return 0
	}
	switch v := meta[versionNumberMetaKey].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	}
	return 0
}

// numberDatasetVersions — 같은 dataset version들의 display 번호를 versionID->number로.
// 우선순위: metadata 저장값(생성 시 부여, 삭제해도 불변) → 없으면(legacy) created_at
// ASC rank fallback. 목록 정렬과 무관하게 항상 생성순 번호를 준다.
func numberDatasetVersions(versions []domain.DatasetVersion) map[string]int {
	asc := make([]domain.DatasetVersion, len(versions))
	copy(asc, versions)
	sort.SliceStable(asc, func(i, j int) bool {
		if asc[i].CreatedAt.Equal(asc[j].CreatedAt) {
			return asc[i].DatasetVersionID < asc[j].DatasetVersionID
		}
		return asc[i].CreatedAt.Before(asc[j].CreatedAt)
	})
	out := make(map[string]int, len(asc))
	for i := range asc {
		n := storedVersionNumber(asc[i].Metadata)
		if n == 0 {
			n = i + 1 // legacy(미저장) fallback — operator migration 0003로 영구화 가능.
		}
		out[asc[i].DatasetVersionID] = n
	}
	return out
}

// nextVersionNumber — 새 version에 부여할 생성순 번호 (기존 최대 + 1).
func nextVersionNumber(existing []domain.DatasetVersion) int {
	max := 0
	for _, n := range numberDatasetVersions(existing) {
		if n > max {
			max = n
		}
	}
	return max + 1
}

func (s *DatasetService) GetDatasetVersionDetail(projectID, datasetID, datasetVersionID string) (domain.DatasetVersionDetail, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetVersionDetail{}, err
	}
	detail := summarizeDatasetVersionDetail(version)
	// stored 번호 우선. legacy(미저장)면 sibling들과 함께 created_at rank로 계산.
	detail.VersionNumber = storedVersionNumber(version.Metadata)
	if detail.VersionNumber == 0 {
		if siblings, sErr := s.store.ListDatasetVersions(projectID, datasetID); sErr == nil {
			detail.VersionNumber = numberDatasetVersions(siblings)[datasetVersionID]
		}
	}
	return detail, nil
}

// summarizeDatasetVersionDetail — GET /versions/{version_id} 응답용 변환.
// 운영자가 detail에서 보고 싶은 것: 각 stage 결과 + 파일 형태 요약. 내부
// URI/artifacts/build_jobs/profile 등 노출 안 함.
func summarizeDatasetVersionDetail(version domain.DatasetVersion) domain.DatasetVersionDetail {
	detail := domain.DatasetVersionDetail{
		DatasetVersionID: version.DatasetVersionID,
		CreatedAt:        version.CreatedAt,
		ReadyAt:          version.ReadyAt,
		IsActive:         version.IsActive,
		Columns:          []string{},
		Clean: domain.DatasetVersionStageDetail{
			Status:      version.CleanStatus,
			CompletedAt: version.CleanedAt,
		},
		DocGenuineness: domain.DatasetVersionStageDetail{
			Status:      metadataString(version.Metadata, "doc_genuineness_status", ""),
			CompletedAt: optionalMetadataTime(version.Metadata, "doc_genuineness_completed_at"),
		},
		ClauseLabel: domain.DatasetVersionStageDetail{
			Status:      metadataString(version.Metadata, "clause_label_status", ""),
			CompletedAt: optionalMetadataTime(version.Metadata, "clause_label_completed_at"),
		},
	}
	if version.CleanSummary != nil {
		detail.Clean.Summary = version.CleanSummary
	}
	if summary, ok := version.Metadata["doc_genuineness_summary"]; ok {
		// silverone 2026-05-28 (C 옵션) — build-detail과 동일한 normalized shape
		// (`genuineness` + `total`)으로 응답. raw tier_counts 키는 제거하지만
		// 저장된 metadata 자체는 건드리지 않는다.
		detail.DocGenuineness.Summary = normalizeDocGenuinenessSummary(summary)
	}
	if summary, ok := version.Metadata["clause_label_summary"]; ok {
		// 동일 — aspect / sentiment / total로 정리, raw *_counts / clause_count
		// 키는 제거. taxonomy_id 등 부수 필드는 보존.
		detail.ClauseLabel.Summary = normalizeClauseLabelSummary(summary)
	}
	if version.SourceSummary != nil {
		if version.SourceSummary.RowCount != nil {
			detail.RowCount = *version.SourceSummary.RowCount
		}
		detail.ColumnCount = version.SourceSummary.ColumnCount
		for _, col := range version.SourceSummary.Columns {
			detail.Columns = append(detail.Columns, col.Name)
		}
	}
	if upload, ok := version.Metadata["upload"].(map[string]any); ok {
		switch b := upload["byte_size"].(type) {
		case int64:
			detail.ByteSize = b
		case int:
			detail.ByteSize = int64(b)
		case float64:
			detail.ByteSize = int64(b)
		}
	}
	return detail
}

// optionalMetadataTime — metadataTime(...)을 *time.Time으로 래핑. 값 없거나
// zero면 nil로 노출해 omitempty 처리한다.
func optionalMetadataTime(meta map[string]any, key string) *time.Time {
	t, ok := metadataTime(meta, key)
	if !ok || t.IsZero() {
		return nil
	}
	return &t
}

// summarizeDatasetVersionForList — version 목록 응답용 요약. 파일 형태 정보는
// storageURI에서 DuckDB로 한 번 더 파싱한다(sample row 생략). versions 수가
// 많아지면 캐싱이나 lazy 필드로 옮기는 걸 검토할 것.
func summarizeDatasetVersionForList(version domain.DatasetVersion) domain.DatasetVersionListItem {
	item := domain.DatasetVersionListItem{
		DatasetVersionID:     version.DatasetVersionID,
		CreatedAt:            version.CreatedAt,
		IsActive:             version.IsActive,
		Columns:              []string{},
		CleanStatus:          version.CleanStatus,
		DocGenuinenessStatus: metadataString(version.Metadata, "doc_genuineness_status", ""),
		ClauseLabelStatus:    metadataString(version.Metadata, "clause_label_status", ""),
	}
	item.OriginalFilename = metadataNestedString(version.Metadata, "upload", "original_filename")
	if item.OriginalFilename == "" {
		item.OriginalFilename = metadataNestedString(version.Metadata, "upload", "stored_filename")
	}
	summary := loadDatasetSourceSummary(version.StorageURI, 0)
	if summary != nil {
		if summary.RowCount != nil {
			item.RowCount = *summary.RowCount
		}
		item.ColumnCount = summary.ColumnCount
		for _, col := range summary.Columns {
			item.Columns = append(item.Columns, col.Name)
		}
	}
	if upload, ok := version.Metadata["upload"].(map[string]any); ok {
		switch b := upload["byte_size"].(type) {
		case int64:
			item.ByteSize = b
		case int:
			item.ByteSize = int64(b)
		case float64:
			item.ByteSize = int64(b)
		}
	}
	return item
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
