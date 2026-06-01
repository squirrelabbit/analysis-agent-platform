package service

import (
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

const (
	datasetBuildStageSource = "source"
	// dataset_build 7 task 제거 후에도 stage view는 metadata 기반 status 표시를
	// 유지 (DatasetVersion struct field는 별도 task로 정리). 따라서 stage 이름은
	// 여기서만 사용되는 로컬 상수로 보존.
	datasetBuildTypePrepare   = "prepare"
	datasetBuildTypeSentiment = "sentiment"
	datasetBuildTypeEmbedding = "embedding"
	datasetBuildTypeCluster   = "cluster"
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
	blockedReason := datasetVersionStageBlockedReason(version, stage)
	canRun := datasetVersionStageCanRun(stage, state, latestJob, blockedReason)
	stageView := domain.DatasetVersionBuildStage{
		Stage:           stage,
		Status:          state.status,
		Applicable:      state.applicable,
		Required:        state.required,
		Ready:           state.ready,
		DependsOn:       datasetVersionStageDependsOn(stage),
		CanRun:          canRun,
		RunGroup:        datasetVersionStageRunGroup(stage),
		AutoRunEligible: datasetVersionStageAutoRunEligible(version, stage, state, canRun),
		Artifacts:       cloneBuildStageArtifacts(artifacts),
	}
	if blockedReason != "" {
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
	case datasetBuildTypePrepare, datasetBuildTypeSentiment, datasetBuildTypeEmbedding:
		// silverone 2026-05-28 (β2 cleanup PR2) — prepare/sentiment/embedding
		// stage는 ADR-018 β2로 사라졌다. struct 필드 의존 분기 stub. stage view
		// 자체를 어디서 노출할지(frontend stage enum)는 PR3에서 정리.
		return datasetVersionStageRuntimeState{status: "not_applicable"}
	case datasetBuildTypeCluster:
		status := strings.TrimSpace(metadataString(version.Metadata, "cluster_status", ""))
		return datasetVersionStageRuntimeState{
			status:     datasetVersionStatusOrNotApplicable(status, unstructured),
			applicable: unstructured,
			required:   metadataBool(version.Metadata, "cluster_required") || datasetVersionStageRequested(status),
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

func datasetVersionStageDependsOn(stage string) []string {
	switch stage {
	case datasetBuildTypeClean:
		return []string{datasetBuildStageSource}
	case datasetBuildTypePrepare:
		return []string{datasetBuildTypeClean}
	case datasetBuildTypeSentiment, datasetBuildTypeEmbedding:
		return []string{datasetBuildTypePrepare}
	case datasetBuildTypeCluster:
		return []string{datasetBuildTypeEmbedding}
	default:
		return []string{}
	}
}

func datasetVersionStageRunGroup(stage string) string {
	switch stage {
	case datasetBuildStageSource:
		return "source"
	case datasetBuildTypeClean:
		return "pre_prepare"
	case datasetBuildTypePrepare:
		return "prepare"
	case datasetBuildTypeSentiment, datasetBuildTypeEmbedding:
		return "post_prepare"
	case datasetBuildTypeCluster:
		return "post_embedding"
	default:
		return ""
	}
}

func datasetVersionStageCanRun(stage string, state datasetVersionStageRuntimeState, latestJob *domain.DatasetVersionBuildJobStatus, blockedReason string) bool {
	if stage == datasetBuildStageSource || !state.applicable {
		return false
	}
	if latestJob != nil && datasetBuildJobActive(latestJob.Status) {
		return false
	}
	return strings.TrimSpace(blockedReason) == ""
}

func datasetBuildJobActive(status string) bool {
	switch strings.TrimSpace(status) {
	case "queued", "running":
		return true
	default:
		return false
	}
}

func datasetVersionStageAutoRunEligible(version domain.DatasetVersion, stage string, state datasetVersionStageRuntimeState, canRun bool) bool {
	if !canRun || state.ready {
		return false
	}
	switch stage {
	case datasetBuildTypeClean:
		return requiresClean(version) && len(resolveDatasetBuildTextSelection(version.Metadata, nil).Columns) > 0
	case datasetBuildTypePrepare:
		return metadataBool(version.Metadata, "prepare_required")
	case datasetBuildTypeSentiment:
		return metadataBool(version.Metadata, "sentiment_required")
	case datasetBuildTypeEmbedding:
		// β2 dead stage — auto-run 안 일어남.
		return false
	case datasetBuildTypeCluster:
		return metadataBool(version.Metadata, "cluster_required")
	default:
		return false
	}
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
	// silverone 2026-05-28 (β2 cleanup PR2) — prepare/sentiment/embedding model
	// 필드는 제거. stage view에서 model은 metadata.*_model로 fallback.
	if stage == "" {
		return ""
	}
	return metadataString(version.Metadata, stage+"_model", "")
}

func datasetVersionStagePromptVersion(version domain.DatasetVersion, stage string) string {
	// silverone 2026-05-28 (β2 cleanup PR2) — prepare/sentiment prompt 필드 제거.
	// stage view에서 prompt_version은 metadata.*_prompt_version로 fallback.
	if stage == "" {
		return ""
	}
	return metadataString(version.Metadata, stage+"_prompt_version", "")
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
