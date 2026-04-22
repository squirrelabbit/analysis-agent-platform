package service

import (
	"os"
	"path/filepath"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

func (s *DatasetService) ResolveSourceDownload(projectID, datasetID, datasetVersionID string) (string, string, string, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return "", "", "", err
	}
	if metadataString(version.Metadata, "storage_backend", "") != "local_fs" || metadataString(version.Metadata, "storage_scope", "") != "dataset_upload" {
		return "", "", "", ErrInvalidArgument{Message: "source download supports uploaded dataset versions only"}
	}

	sourcePath := strings.TrimSpace(version.StorageURI)
	if sourcePath == "" {
		return "", "", "", ErrInvalidArgument{Message: "storage_uri is required"}
	}
	absolutePath, err := filepath.Abs(sourcePath)
	if err != nil {
		return "", "", "", err
	}
	if !s.isDatasetUploadSourcePath(projectID, datasetID, datasetVersionID, absolutePath) {
		return "", "", "", ErrInvalidArgument{Message: "source file is outside dataset upload storage"}
	}
	info, statErr := os.Stat(absolutePath)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return "", "", "", ErrNotFound{Resource: "source file"}
		}
		return "", "", "", statErr
	}
	if info.IsDir() {
		return "", "", "", ErrInvalidArgument{Message: "source file must be a file"}
	}

	filename := sanitizeFilename(metadataNestedString(version.Metadata, "upload", "original_filename"))
	if filename == "" {
		filename = sanitizeFilename(metadataNestedString(version.Metadata, "upload", "stored_filename"))
	}
	if filename == "" {
		filename = filepath.Base(absolutePath)
	}
	contentType := strings.TrimSpace(metadataNestedString(version.Metadata, "upload", "content_type"))
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	return absolutePath, filename, contentType, nil
}

func (s *DatasetService) GetPreparePreview(
	projectID, datasetID, datasetVersionID string,
	input domain.DatasetPreparePreviewQuery,
) (domain.DatasetPreparePreviewResponse, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetPreparePreviewResponse{}, err
	}

	preparedRef, prepareFormat, err := resolvePrepareArtifact(version)
	if err != nil {
		return domain.DatasetPreparePreviewResponse{}, err
	}
	if prepareFormat != "parquet" {
		return domain.DatasetPreparePreviewResponse{}, ErrInvalidArgument{Message: "prepare preview supports parquet artifact only"}
	}

	limit := defaultPreparePreviewLimit
	if input.Limit != nil {
		limit = *input.Limit
	}
	if limit <= 0 {
		return domain.DatasetPreparePreviewResponse{}, ErrInvalidArgument{Message: "limit must be a positive integer"}
	}
	if limit > maxPreparePreviewLimit {
		limit = maxPreparePreviewLimit
	}

	samples, err := loadPrepareSamplesFromParquet(preparedRef, limit, "")
	if err != nil {
		return domain.DatasetPreparePreviewResponse{}, err
	}

	rawTextColumn := metadataString(version.Metadata, "raw_text_column", metadataString(version.Metadata, "text_column", "text"))
	rawTextColumns := metadataStringList(version.Metadata, "raw_text_columns")
	if len(rawTextColumns) == 0 {
		rawTextColumns = metadataStringList(version.Metadata, "text_columns")
	}
	if len(rawTextColumns) == 0 && strings.TrimSpace(rawTextColumn) != "" {
		rawTextColumns = []string{rawTextColumn}
	}
	textJoiner, _ := metadataRawString(version.Metadata, "text_joiner")

	response := domain.DatasetPreparePreviewResponse{
		ProjectID:          projectID,
		DatasetID:          datasetID,
		DatasetVersionID:   datasetVersionID,
		PrepareStatus:      version.PrepareStatus,
		PreparedAt:         version.PreparedAt,
		PreparedRef:        preparedRef,
		PrepareFormat:      prepareFormat,
		RawTextColumn:      rawTextColumn,
		RawTextColumns:     rawTextColumns,
		TextJoiner:         textJoiner,
		PreparedTextColumn: metadataString(version.Metadata, "prepared_text_column", "normalized_text"),
		RowIDColumn:        metadataString(version.Metadata, "row_id_column", "row_id"),
		Summary:            clonePrepareSummary(version.PrepareSummary),
		SampleLimit:        limit,
		Samples:            samples,
	}

	if response.Summary != nil && response.Summary.ReviewCount > 0 {
		reviewLimit := response.Summary.ReviewCount
		if reviewLimit > limit {
			reviewLimit = limit
		}
		reviewSamples, err := loadPrepareSamplesFromParquet(preparedRef, reviewLimit, "review")
		if err != nil {
			return domain.DatasetPreparePreviewResponse{}, err
		}
		response.WarningPanel = &domain.DatasetPrepareWarningPanel{
			ReviewCount: response.Summary.ReviewCount,
			Samples:     reviewSamples,
		}
	}

	return response, nil
}

func (s *DatasetService) ResolvePrepareDownload(projectID, datasetID, datasetVersionID string) (string, string, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return "", "", err
	}
	preparedRef, prepareFormat, err := resolvePrepareArtifact(version)
	if err != nil {
		return "", "", err
	}
	if prepareFormat != "parquet" {
		return "", "", ErrInvalidArgument{Message: "prepare download supports parquet artifact only"}
	}
	info, statErr := os.Stat(preparedRef)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return "", "", ErrNotFound{Resource: "prepare artifact"}
		}
		return "", "", statErr
	}
	if info.IsDir() {
		return "", "", ErrInvalidArgument{Message: "prepare artifact must be a file"}
	}
	exportPath, err := exportPrepareCSVFromParquet(preparedRef)
	if err != nil {
		return "", "", err
	}
	filename := strings.TrimSpace(filepath.Base(preparedRef))
	if filename == "" || filename == "." || filename == string(filepath.Separator) {
		filename = "prepared.csv"
	} else if strings.HasSuffix(strings.ToLower(filename), ".parquet") {
		filename = filename[:len(filename)-len(".parquet")] + ".csv"
	} else {
		filename = filename + ".csv"
	}
	return exportPath, filename, nil
}

func (s *DatasetService) GetSentimentPreview(
	projectID, datasetID, datasetVersionID string,
	input domain.DatasetSentimentPreviewQuery,
) (domain.DatasetSentimentPreviewResponse, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetSentimentPreviewResponse{}, err
	}

	sentimentRef, sentimentFormat, err := resolveSentimentArtifact(version)
	if err != nil {
		return domain.DatasetSentimentPreviewResponse{}, err
	}
	if sentimentFormat != "parquet" {
		return domain.DatasetSentimentPreviewResponse{}, ErrInvalidArgument{Message: "sentiment preview supports parquet artifact only"}
	}

	limit := defaultSentimentPreviewLimit
	if input.Limit != nil {
		limit = *input.Limit
	}
	if limit <= 0 {
		return domain.DatasetSentimentPreviewResponse{}, ErrInvalidArgument{Message: "limit must be a positive integer"}
	}
	if limit > maxSentimentPreviewLimit {
		limit = maxSentimentPreviewLimit
	}

	samples, err := loadSentimentSamplesFromParquet(sentimentRef, limit)
	if err != nil {
		return domain.DatasetSentimentPreviewResponse{}, err
	}

	sentimentTextColumn := metadataString(version.Metadata, "sentiment_text_column", defaultPreparedTextColumn(version))
	sentimentTextColumns := metadataStringList(version.Metadata, "sentiment_text_columns")
	if len(sentimentTextColumns) == 0 && strings.TrimSpace(sentimentTextColumn) != "" {
		sentimentTextColumns = []string{sentimentTextColumn}
	}
	sentimentTextJoiner, _ := metadataRawString(version.Metadata, "sentiment_text_joiner")

	response := domain.DatasetSentimentPreviewResponse{
		ProjectID:                 projectID,
		DatasetID:                 datasetID,
		DatasetVersionID:          datasetVersionID,
		SentimentStatus:           version.SentimentStatus,
		SentimentLabeledAt:        version.SentimentLabeledAt,
		SentimentRef:              sentimentRef,
		SentimentFormat:           sentimentFormat,
		SentimentTextColumn:       sentimentTextColumn,
		SentimentTextColumns:      sentimentTextColumns,
		TextJoiner:                sentimentTextJoiner,
		SentimentLabelColumn:      metadataString(version.Metadata, "sentiment_label_column", "sentiment_label"),
		SentimentConfidenceColumn: metadataString(version.Metadata, "sentiment_confidence_column", "sentiment_confidence"),
		SentimentReasonColumn:     metadataString(version.Metadata, "sentiment_reason_column", "sentiment_reason"),
		RowIDColumn:               metadataString(version.Metadata, "row_id_column", "row_id"),
		Summary:                   cloneSentimentSummary(buildSentimentSummary(version.Metadata)),
		SampleLimit:               limit,
		Samples:                   samples,
	}

	return response, nil
}

func (s *DatasetService) ResolveSentimentDownload(projectID, datasetID, datasetVersionID string) (string, string, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return "", "", err
	}
	sentimentRef, sentimentFormat, err := resolveSentimentArtifact(version)
	if err != nil {
		return "", "", err
	}
	if sentimentFormat != "parquet" {
		return "", "", ErrInvalidArgument{Message: "sentiment download supports parquet artifact only"}
	}
	info, statErr := os.Stat(sentimentRef)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return "", "", ErrNotFound{Resource: "sentiment artifact"}
		}
		return "", "", statErr
	}
	if info.IsDir() {
		return "", "", ErrInvalidArgument{Message: "sentiment artifact must be a file"}
	}
	exportPath, err := exportSentimentCSVFromParquet(sentimentRef)
	if err != nil {
		return "", "", err
	}
	filename := strings.TrimSpace(filepath.Base(sentimentRef))
	if filename == "" || filename == "." || filename == string(filepath.Separator) {
		filename = "sentiment.csv"
	} else if strings.HasSuffix(strings.ToLower(filename), ".parquet") {
		filename = filename[:len(filename)-len(".parquet")] + ".csv"
	} else {
		filename = filename + ".csv"
	}
	return exportPath, filename, nil
}

func (s *DatasetService) GetClusterMembers(
	projectID, datasetID, datasetVersionID, clusterID string,
	input domain.DatasetClusterMembersQuery,
) (domain.DatasetClusterMembersResponse, error) {
	clusterID = strings.TrimSpace(clusterID)
	if clusterID == "" {
		return domain.DatasetClusterMembersResponse{}, ErrInvalidArgument{Message: "cluster_id is required"}
	}
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetClusterMembersResponse{}, err
	}

	clusterSummaryRef := strings.TrimSpace(metadataString(version.Metadata, "cluster_summary_ref", ""))
	if clusterSummaryRef == "" {
		clusterSummaryRef = strings.TrimSpace(metadataString(version.Metadata, "cluster_ref", ""))
	}
	if clusterSummaryRef == "" {
		return domain.DatasetClusterMembersResponse{}, ErrInvalidArgument{Message: "cluster summary artifact is not ready"}
	}
	clusterMembershipRef := strings.TrimSpace(metadataString(version.Metadata, "cluster_membership_ref", ""))
	if clusterMembershipRef == "" {
		clusterMembershipRef = deriveClusterMembershipURI(clusterSummaryRef)
	}
	if clusterMembershipRef == "" {
		return domain.DatasetClusterMembersResponse{}, ErrInvalidArgument{Message: "cluster membership artifact is not ready"}
	}

	clusterSummary, err := loadClusterSummary(clusterSummaryRef, clusterID)
	if err != nil {
		return domain.DatasetClusterMembersResponse{}, err
	}

	limit := defaultClusterMembersLimit
	if input.Limit != nil {
		limit = *input.Limit
	}
	if limit <= 0 {
		return domain.DatasetClusterMembersResponse{}, ErrInvalidArgument{Message: "limit must be a positive integer"}
	}
	if limit > maxClusterMembersLimit {
		limit = maxClusterMembersLimit
	}
	samplesOnly := input.SamplesOnly != nil && *input.SamplesOnly

	items, totalCount, sampleCount, err := loadClusterMembersFromParquet(clusterMembershipRef, clusterID, limit, samplesOnly)
	if err != nil {
		return domain.DatasetClusterMembersResponse{}, err
	}
	return domain.DatasetClusterMembersResponse{
		ProjectID:            projectID,
		DatasetID:            datasetID,
		DatasetVersionID:     datasetVersionID,
		ClusterID:            clusterID,
		ClusterSummaryRef:    clusterSummaryRef,
		ClusterMembershipRef: clusterMembershipRef,
		Limit:                limit,
		SamplesOnly:          samplesOnly,
		TotalCount:           totalCount,
		SampleCount:          sampleCount,
		Cluster:              clusterSummary,
		Items:                items,
	}, nil
}
