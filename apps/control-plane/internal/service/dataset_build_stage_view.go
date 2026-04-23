package service

import (
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

const (
	datasetBuildStageSource = "source"
)

func buildDatasetVersionStages(version domain.DatasetVersion, buildJobs []domain.DatasetVersionBuildJobStatus) []domain.DatasetVersionBuildStage {
	artifactsByStage := datasetVersionArtifactsByStage(version.Artifacts)
	jobsByType := datasetVersionBuildJobsByType(buildJobs)
	stages := []domain.DatasetVersionBuildStage{
		buildDatasetVersionStage(version, datasetBuildStageSource, artifactsByStage[datasetBuildStageSource], nil),
		buildDatasetVersionStage(version, datasetBuildTypeClean, artifactsByStage[datasetBuildTypeClean], jobsByType[datasetBuildTypeClean]),
		buildDatasetVersionStage(version, datasetBuildTypePrepare, artifactsByStage[datasetBuildTypePrepare], jobsByType[datasetBuildTypePrepare]),
		buildDatasetVersionStage(version, datasetBuildTypeSentiment, artifactsByStage[datasetBuildTypeSentiment], jobsByType[datasetBuildTypeSentiment]),
		buildDatasetVersionStage(version, datasetBuildTypeEmbedding, artifactsByStage[datasetBuildTypeEmbedding], jobsByType[datasetBuildTypeEmbedding]),
		buildDatasetVersionStage(version, datasetBuildTypeCluster, artifactsByStage[datasetBuildTypeCluster], jobsByType[datasetBuildTypeCluster]),
	}
	return stages
}

func buildDatasetVersionStage(
	version domain.DatasetVersion,
	stage string,
	artifacts []domain.DatasetVersionArtifact,
	latestJob *domain.DatasetVersionBuildJobStatus,
) domain.DatasetVersionBuildStage {
	state := datasetVersionStageState(version, stage)
	stageView := domain.DatasetVersionBuildStage{
		Stage:      stage,
		Status:     state.status,
		Applicable: state.applicable,
		Required:   state.required,
		Ready:      state.ready,
		Artifacts:  cloneBuildStageArtifacts(artifacts),
	}
	if blockedReason := datasetVersionStageBlockedReason(version, stage); blockedReason != "" {
		stageView.BlockedReason = stringPointer(blockedReason)
	}
	if latestJob != nil {
		job := *latestJob
		stageView.LatestJob = &job
		stageView.ErrorMessage = cloneStringPointer(job.ErrorMessage)
		stageView.Diagnostics = job.Diagnostics
	}
	if stageView.ErrorMessage == nil {
		if errorMessage := datasetVersionStageError(version, stage); errorMessage != "" {
			stageView.ErrorMessage = stringPointer(errorMessage)
		}
	}
	if primaryArtifact, ok := datasetVersionPrimaryArtifact(stage, artifacts); ok {
		artifact := primaryArtifact
		stageView.PrimaryArtifact = &artifact
		stageView.Summary = cloneBuildStageMap(primaryArtifact.Summary)
		stageView.Model = strings.TrimSpace(primaryArtifact.Model)
		stageView.PromptVersion = strings.TrimSpace(primaryArtifact.PromptVersion)
	}
	if stageView.Summary == nil {
		stageView.Summary = datasetVersionStageSummary(version, stage)
	}
	if stageView.Model == "" {
		stageView.Model = datasetVersionStageModel(version, stage)
	}
	if stageView.PromptVersion == "" {
		stageView.PromptVersion = datasetVersionStagePromptVersion(version, stage)
	}
	return stageView
}

type datasetVersionStageRuntimeState struct {
	status     string
	applicable bool
	required   bool
	ready      bool
}

func datasetVersionStageState(version domain.DatasetVersion, stage string) datasetVersionStageRuntimeState {
	unstructured := datasetVersionSupportsTextPipeline(version)
	switch stage {
	case datasetBuildStageSource:
		status := "not_available"
		ready := strings.TrimSpace(version.StorageURI) != ""
		if ready {
			status = "ready"
		}
		return datasetVersionStageRuntimeState{status: status, applicable: true, required: true, ready: ready}
	case datasetBuildTypeClean:
		return datasetVersionStageRuntimeState{
			status:     cleanStatus(version),
			applicable: unstructured,
			required:   requiresClean(version),
			ready:      isCleanReady(version),
		}
	case datasetBuildTypePrepare:
		return datasetVersionStageRuntimeState{
			status:     datasetVersionStatusOrNotApplicable(version.PrepareStatus, unstructured),
			applicable: unstructured,
			required:   requiresPrepare(version),
			ready:      isPrepareReady(version),
		}
	case datasetBuildTypeSentiment:
		return datasetVersionStageRuntimeState{
			status:     datasetVersionStatusOrNotApplicable(version.SentimentStatus, unstructured),
			applicable: unstructured,
			required:   requiresSentiment(version),
			ready:      isSentimentReady(version),
		}
	case datasetBuildTypeEmbedding:
		return datasetVersionStageRuntimeState{
			status:     datasetVersionStatusOrNotApplicable(version.EmbeddingStatus, unstructured),
			applicable: unstructured,
			required:   datasetVersionStageRequested(version.EmbeddingStatus),
			ready:      embeddingBuildReady(version),
		}
	case datasetBuildTypeCluster:
		status := strings.TrimSpace(metadataString(version.Metadata, "cluster_status", ""))
		return datasetVersionStageRuntimeState{
			status:     datasetVersionStatusOrNotApplicable(status, unstructured),
			applicable: unstructured,
			required:   datasetVersionStageRequested(status),
			ready:      datasetClusterReady(version),
		}
	default:
		return datasetVersionStageRuntimeState{status: "not_applicable"}
	}
}

func datasetVersionSupportsTextPipeline(version domain.DatasetVersion) bool {
	switch version.DataType {
	case "unstructured", "mixed", "both":
		return true
	default:
		return false
	}
}

func datasetVersionStatusOrNotApplicable(status string, applicable bool) string {
	status = strings.TrimSpace(status)
	if !applicable {
		return "not_applicable"
	}
	if status == "" {
		return "not_requested"
	}
	return status
}

func datasetVersionStageRequested(status string) bool {
	status = strings.TrimSpace(status)
	return status != "" && status != "not_requested" && status != "not_applicable"
}

func datasetVersionStageBlockedReason(version domain.DatasetVersion, stage string) string {
	if !datasetVersionSupportsTextPipeline(version) {
		switch stage {
		case datasetBuildTypeClean, datasetBuildTypePrepare, datasetBuildTypeSentiment, datasetBuildTypeEmbedding, datasetBuildTypeCluster:
			return "structured dataset version does not use text build stages"
		}
	}
	switch stage {
	case datasetBuildTypeClean:
		if len(resolveDatasetBuildTextSelection(version.Metadata, nil).Columns) == 0 {
			return "text_columns is required before clean"
		}
	case datasetBuildTypePrepare:
		switch cleanStatus(version) {
		case "queued", "cleaning", "failed", "stale":
			return "clean stage must be ready before prepare"
		}
	case datasetBuildTypeSentiment:
		if requiresPrepare(version) && !isPrepareReady(version) {
			return "prepare stage must be ready before sentiment"
		}
	case datasetBuildTypeEmbedding:
		if requiresPrepare(version) && !isPrepareReady(version) {
			return "prepare stage must be ready before embedding"
		}
	case datasetBuildTypeCluster:
		if !embeddingBuildReady(version) {
			return "embedding stage must be ready before cluster"
		}
	}
	return ""
}

func datasetVersionStageError(version domain.DatasetVersion, stage string) string {
	switch stage {
	case datasetBuildTypeClean:
		return metadataString(version.Metadata, "clean_error", "")
	case datasetBuildTypePrepare:
		return metadataString(version.Metadata, "prepare_error", "")
	case datasetBuildTypeSentiment:
		return metadataString(version.Metadata, "sentiment_error", "")
	case datasetBuildTypeEmbedding:
		return metadataString(version.Metadata, "embedding_error", "")
	case datasetBuildTypeCluster:
		return metadataString(version.Metadata, "cluster_error", "")
	default:
		return ""
	}
}

func datasetVersionStageSummary(version domain.DatasetVersion, stage string) map[string]any {
	if version.Metadata == nil {
		return nil
	}
	key := stage + "_summary"
	if stage == datasetBuildStageSource {
		return nil
	}
	raw, ok := version.Metadata[key].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	return cloneBuildStageMap(raw)
}

func datasetVersionStageModel(version domain.DatasetVersion, stage string) string {
	switch stage {
	case datasetBuildTypePrepare:
		return derefString(version.PrepareModel)
	case datasetBuildTypeSentiment:
		return derefString(version.SentimentModel)
	case datasetBuildTypeEmbedding:
		return derefString(version.EmbeddingModel)
	default:
		return ""
	}
}

func datasetVersionStagePromptVersion(version domain.DatasetVersion, stage string) string {
	switch stage {
	case datasetBuildTypePrepare:
		return derefString(version.PreparePromptVer)
	case datasetBuildTypeSentiment:
		return derefString(version.SentimentPromptVer)
	default:
		return ""
	}
}

func datasetVersionPrimaryArtifact(stage string, artifacts []domain.DatasetVersionArtifact) (domain.DatasetVersionArtifact, bool) {
	preferredTypes := map[string][]string{
		datasetBuildStageSource: {datasetBuildStageSource},
		datasetBuildTypeClean:   {"clean"},
		datasetBuildTypePrepare: {"prepare"},
		datasetBuildTypeSentiment: {
			"sentiment",
		},
		datasetBuildTypeEmbedding: {
			"embedding_index",
			"embedding",
			"embedding_chunks",
		},
		datasetBuildTypeCluster: {
			"cluster_summary",
			"cluster_membership",
		},
	}
	for _, artifactType := range preferredTypes[stage] {
		for _, artifact := range artifacts {
			if artifact.ArtifactType == artifactType {
				return artifact, true
			}
		}
	}
	if len(artifacts) > 0 {
		return artifacts[0], true
	}
	return domain.DatasetVersionArtifact{}, false
}

func datasetVersionArtifactsByStage(artifacts []domain.DatasetVersionArtifact) map[string][]domain.DatasetVersionArtifact {
	result := make(map[string][]domain.DatasetVersionArtifact)
	for _, artifact := range artifacts {
		stage := strings.TrimSpace(artifact.Stage)
		if stage == "" {
			continue
		}
		result[stage] = append(result[stage], artifact)
	}
	return result
}

func datasetVersionBuildJobsByType(jobs []domain.DatasetVersionBuildJobStatus) map[string]*domain.DatasetVersionBuildJobStatus {
	result := make(map[string]*domain.DatasetVersionBuildJobStatus)
	for index := range jobs {
		job := jobs[index]
		if strings.TrimSpace(job.BuildType) == "" {
			continue
		}
		result[job.BuildType] = &job
	}
	return result
}

func cloneBuildStageArtifacts(items []domain.DatasetVersionArtifact) []domain.DatasetVersionArtifact {
	if len(items) == 0 {
		return nil
	}
	cloned := make([]domain.DatasetVersionArtifact, 0, len(items))
	for _, item := range items {
		item.Summary = cloneBuildStageMap(item.Summary)
		item.Metadata = cloneBuildStageMap(item.Metadata)
		cloned = append(cloned, item)
	}
	return cloned
}

func cloneBuildStageMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}
