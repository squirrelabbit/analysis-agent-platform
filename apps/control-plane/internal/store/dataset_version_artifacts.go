package store

import (
	"encoding/json"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

func deriveDatasetVersionArtifacts(version domain.DatasetVersion, now time.Time) []domain.DatasetVersionArtifact {
	version = normalizeDatasetVersionCleanFields(version)
	items := make([]domain.DatasetVersionArtifact, 0, 10)

	sourceMetadata := map[string]any{
		"data_type": version.DataType,
	}
	if version.RecordCount != nil {
		sourceMetadata["record_count"] = *version.RecordCount
	}
	items = append(items, datasetVersionArtifact(version, "source", "source", "ready", strings.TrimSpace(version.StorageURI), inferDatasetArtifactFormat(version.StorageURI, ""), "", "", nil, sourceMetadata, now))

	cleanStatus := strings.TrimSpace(version.CleanStatus)
	cleanURI := firstNonEmpty(derefString(version.CleanURI), metadataString(version.Metadata, "clean_uri"), metadataString(version.Metadata, "cleaned_ref"))
	if shouldIncludeDatasetArtifact(cleanStatus, cleanURI) {
		cleanFormat := firstNonEmpty(metadataString(version.Metadata, "clean_format"), metadataString(version.Metadata, "cleaned_format"), inferDatasetArtifactFormat(cleanURI, "parquet"))
		items = append(items, datasetVersionArtifact(
			version,
			"clean",
			"clean",
			cleanStatus,
			cleanURI,
			cleanFormat,
			"",
			"",
			artifactSummary(version.Metadata, "clean_summary"),
			artifactMetadata(version.Metadata, []string{
				"clean_progress_ref",
				"cleaned_text_column",
				"raw_text_column",
				"raw_text_columns",
				"text_joiner",
				"clean_notes",
				"clean_error",
				"row_id_column",
			}),
			now,
		))
	}
	if progressRef := metadataString(version.Metadata, "clean_progress_ref"); progressRef != "" {
		items = append(items, datasetVersionArtifact(version, "clean_progress", "clean", cleanStatus, progressRef, inferDatasetArtifactFormat(progressRef, "json"), "", "", nil, nil, now))
	}

	// silverone 2026-05-28 (β2 cleanup PR2) — prepare / sentiment / embedding /
	// embedding_index / embedding_chunks 5 artifact 분기 제거. ADR-018 β2로
	// 단계 자체가 사라져 metadata에 채워지지 않음. cluster artifact는 본 PR
	// scope 밖 (별도 metadata key 사용 — 보존).

	clusterStatus := metadataString(version.Metadata, "cluster_status")
	clusterSummaryRef := firstNonEmpty(metadataString(version.Metadata, "cluster_summary_ref"), metadataString(version.Metadata, "cluster_ref"))
	if shouldIncludeDatasetArtifact(clusterStatus, clusterSummaryRef) {
		clusterSummaryFormat := firstNonEmpty(metadataString(version.Metadata, "cluster_summary_format"), metadataString(version.Metadata, "cluster_format"), inferDatasetArtifactFormat(clusterSummaryRef, "json"))
		items = append(items, datasetVersionArtifact(
			version,
			"cluster_summary",
			"cluster",
			readyStatusForArtifact(clusterStatus, clusterSummaryRef),
			clusterSummaryRef,
			clusterSummaryFormat,
			"",
			"",
			artifactSummary(version.Metadata, "cluster_summary"),
			artifactMetadata(version.Metadata, []string{
				"cluster_ref",
				"cluster_membership_ref",
				"cluster_membership_format",
				"cluster_algorithm",
				"cluster_source_embedding_ref",
				"cluster_similarity_threshold",
				"cluster_top_n",
				"cluster_sample_n",
				"cluster_params_hash",
				"cluster_notes",
				"cluster_error",
				"cluster_stale_reason",
			}),
			now,
		))
	}
	if membershipRef := metadataString(version.Metadata, "cluster_membership_ref"); membershipRef != "" {
		membershipFormat := firstNonEmpty(metadataString(version.Metadata, "cluster_membership_format"), inferDatasetArtifactFormat(membershipRef, "parquet"))
		items = append(items, datasetVersionArtifact(version, "cluster_membership", "cluster", readyStatusForArtifact(clusterStatus, membershipRef), membershipRef, membershipFormat, "", "", nil, artifactMetadata(version.Metadata, []string{"cluster_summary_ref", "cluster_algorithm", "cluster_params_hash"}), now))
	}

	// 5/12 (silverone) — document_cluster_profile은 별도 테이블
	// dataset_version_cluster_profile_builds가 단일 진실 소스. 이 함수는
	// dataset_version_artifacts row를 *derive*하므로 매번 같은 id로 upsert되어
	// build immutability를 표현할 수 없다. Codex 검토 권고 (1) 반영 — derive
	// 블럭 제거. latest build 상태는 GET /document_cluster_profile read model
	// endpoint에서 직접 조회한다 (version.Metadata에 latest pointer만 유지).

	sort.Slice(items, func(i, j int) bool {
		if artifactStageOrder(items[i].Stage) == artifactStageOrder(items[j].Stage) {
			return items[i].ArtifactType < items[j].ArtifactType
		}
		return artifactStageOrder(items[i].Stage) < artifactStageOrder(items[j].Stage)
	})
	return items
}

func datasetVersionArtifact(version domain.DatasetVersion, artifactType, stage, status, uri, format, model, promptVersion string, summary, metadata map[string]any, now time.Time) domain.DatasetVersionArtifact {
	status = readyStatusForArtifact(status, uri)
	return domain.DatasetVersionArtifact{
		ArtifactID:       datasetVersionArtifactID(version.DatasetVersionID, artifactType),
		ProjectID:        version.ProjectID,
		DatasetID:        version.DatasetID,
		DatasetVersionID: version.DatasetVersionID,
		ArtifactType:     artifactType,
		Stage:            stage,
		Status:           status,
		URI:              strings.TrimSpace(uri),
		Format:           strings.TrimSpace(format),
		Model:            strings.TrimSpace(model),
		PromptVersion:    strings.TrimSpace(promptVersion),
		Summary:          cloneAnyMap(summary),
		Metadata:         cloneAnyMap(metadata),
		CreatedAt:        now,
		UpdatedAt:        now,
	}
}

func datasetVersionArtifactID(datasetVersionID, artifactType string) string {
	return strings.TrimSpace(datasetVersionID) + ":" + strings.TrimSpace(artifactType)
}

// datasetVersionArtifactPayloadEqual — silverone 2026-05-28 (B1). UPSERT 시
// no-op update 방지를 위해 *payload field*가 동일한지 비교한다. 같은
// (dataset_version_id, artifact_type) 키면 artifact_id / project_id /
// dataset_id는 reflective하게 동일하므로 비교 대상에서 제외. created_at /
// updated_at은 본 비교에서 제외 (값 동일 여부 판단용).
//
// memory store가 동일 값 재attach 시 updated_at을 보존하는 데 사용. postgres
// store는 같은 의미를 `ON CONFLICT DO UPDATE ... WHERE` SQL로 구현하므로 본
// helper를 직접 호출하지 않는다 (DB가 직접 비교 — `IS DISTINCT FROM`).
func datasetVersionArtifactPayloadEqual(a, b domain.DatasetVersionArtifact) bool {
	if a.Stage != b.Stage ||
		a.Status != b.Status ||
		a.URI != b.URI ||
		a.Format != b.Format ||
		a.Model != b.Model ||
		a.PromptVersion != b.PromptVersion {
		return false
	}
	if !mapsEqualJSON(a.Summary, b.Summary) {
		return false
	}
	if !mapsEqualJSON(a.Metadata, b.Metadata) {
		return false
	}
	return true
}

func mapsEqualJSON(a, b map[string]any) bool {
	aj, errA := json.Marshal(defaultMetadataMap(a))
	bj, errB := json.Marshal(defaultMetadataMap(b))
	if errA != nil || errB != nil {
		return false
	}
	return string(aj) == string(bj)
}

func shouldIncludeDatasetArtifact(status, uri string) bool {
	status = strings.TrimSpace(status)
	if strings.TrimSpace(uri) != "" {
		return true
	}
	return status != "" && status != "not_requested" && status != "not_applicable"
}

func readyStatusForArtifact(status, uri string) string {
	status = strings.TrimSpace(status)
	if status == "" && strings.TrimSpace(uri) != "" {
		return "ready"
	}
	if status == "" {
		return "not_requested"
	}
	return status
}

func artifactSummary(metadata map[string]any, key string) map[string]any {
	value, ok := metadata[key].(map[string]any)
	if !ok || len(value) == 0 {
		return nil
	}
	return cloneAnyMap(value)
}

func artifactMetadata(metadata map[string]any, keys []string) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	output := make(map[string]any)
	for _, key := range keys {
		value, ok := metadata[key]
		if !ok || value == nil {
			continue
		}
		output[key] = cloneAnyValue(value)
	}
	if len(output) == 0 {
		return nil
	}
	return output
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func inferDatasetArtifactFormat(uri, fallback string) string {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return strings.TrimSpace(fallback)
	}
	extension := strings.TrimPrefix(strings.ToLower(filepath.Ext(uri)), ".")
	if extension == "" {
		return strings.TrimSpace(fallback)
	}
	if extension == "jsonl" || extension == "json" || extension == "csv" || extension == "parquet" ||
		extension == "xlsx" || extension == "xlsm" {
		return extension
	}
	return strings.TrimSpace(fallback)
}

func artifactStageOrder(stage string) int {
	switch strings.TrimSpace(stage) {
	case "source":
		return 10
	case "clean":
		return 20
	case "prepare":
		return 30
	case "sentiment":
		return 40
	case "embedding":
		return 50
	case "cluster":
		return 60
	default:
		return 100
	}
}
