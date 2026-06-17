package service

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

// silverone 2026-06-01 (다운로드 파일명 타임스탬프) — 다운로드 파일명에
// KST `_YYYYMMDD_HHMMSS` 접미를 붙인다. source / clean / doc_genuineness /
// clause_label 4 endpoint 공통. 같은 파일을 여러 번 받아도 파일명이 겹치지
// 않게 한다.
//
// 형식: `<base>_20260601_134523.<ext>`. 확장자가 없으면 `<name>_<ts>`.
// Asia/Seoul 로드 실패는 fallback으로 UTC 사용 (서버 timezone 무관).
func appendDownloadTimestamp(name string) string {
	now := time.Now()
	if loc, err := time.LoadLocation("Asia/Seoul"); err == nil {
		now = now.In(loc)
	} else {
		now = now.UTC()
	}
	ts := now.Format("20060102_150405")
	if name == "" {
		return ts
	}
	ext := filepath.Ext(name)
	base := strings.TrimSuffix(name, ext)
	if base == "" {
		// 점으로 시작하는 hidden file (예: ".env") — 그대로 + _ts.
		return name + "_" + ts
	}
	return base + "_" + ts + ext
}

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
	return absolutePath, appendDownloadTimestamp(filename), contentType, nil
}

func (s *DatasetService) ResolveCleanDownload(projectID, datasetID, datasetVersionID string) (string, string, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return "", "", err
	}
	cleanedRef, cleanFormat, err := resolveCleanArtifact(version)
	if err != nil {
		return "", "", err
	}
	if cleanFormat != "parquet" {
		return "", "", ErrInvalidArgument{Message: "clean download supports parquet artifact only"}
	}
	info, statErr := os.Stat(cleanedRef)
	if statErr != nil {
		if os.IsNotExist(statErr) {
			return "", "", ErrNotFound{Resource: "clean artifact"}
		}
		return "", "", statErr
	}
	if info.IsDir() {
		return "", "", ErrInvalidArgument{Message: "clean artifact must be a file"}
	}
	// silverone 2026-06-04 — download/view contract (의도된 구분):
	//   - missing clean artifact → 위 os.Stat 블록에서 404(ErrNotFound) 유지.
	//   - 존재하지만 zero-byte / corrupt / wrong-format → 아래 검증이 400(ErrInvalidArgument).
	// 즉 "파일 없음"과 "파일은 있는데 못 읽음"을 상태코드로 구분한다.
	if err := validateArtifactReadable("clean", cleanedRef, artifactParquet); err != nil {
		return "", "", err
	}
	exportPath, err := exportCleanCSVFromParquet(cleanedRef)
	if err != nil {
		return "", "", err
	}
	filename := strings.TrimSpace(filepath.Base(cleanedRef))
	if filename == "" || filename == "." || filename == string(filepath.Separator) {
		filename = "cleaned.csv"
	} else if strings.HasSuffix(strings.ToLower(filename), ".parquet") {
		filename = filename[:len(filename)-len(".parquet")] + ".csv"
	} else {
		filename = filename + ".csv"
	}
	return exportPath, appendDownloadTimestamp(filename), nil
}

// ResolveDocGenuinenessDownload — doc_genuineness jsonl artifact를 CSV로
// 변환해 임시 파일 경로 + 다운로드 filename 반환. handler가 응답 후 임시
// 파일을 정리한다. silverone 2026-06-01 (다운로드 컬럼 확장) — cleaned.parquet
// LEFT JOIN으로 cleaned_text / raw_text / created_at / source_row_index를
// 함께 포함. clean artifact가 없으면 그 컬럼들은 빈 값. exportDocGenuiness
// EnrichedCSV 참조.
func (s *DatasetService) ResolveDocGenuinenessDownload(projectID, datasetID, datasetVersionID string) (string, string, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return "", "", err
	}
	ref := strings.TrimSpace(metadataString(version.Metadata, "doc_genuineness_ref", ""))
	if ref == "" {
		ref = strings.TrimSpace(metadataString(version.Metadata, "doc_genuineness_uri", ""))
	}
	if ref == "" {
		return "", "", ErrNotFound{Resource: "doc_genuineness artifact"}
	}
	// silverone 2026-06-04 — CSV 변환 전에 jsonl이 읽을 수 있는지 검증.
	if err := validateArtifactReadable("doc_genuineness", ref, artifactJSONL); err != nil {
		return "", "", err
	}
	cleanRef := strings.TrimSpace(metadataString(version.Metadata, "cleaned_ref", ""))
	if cleanRef == "" {
		cleanRef = strings.TrimSpace(metadataString(version.Metadata, "clean_uri", ""))
	}
	// verify(교차모델, ADR-026) 모드는 artifact schema가 달라(final_label + model
	// A/B/judge, genuineness 컬럼 없음) 전용 exporter를 쓴다. 단일 모델 경로 유지.
	var exportPath string
	if metadataString(version.Metadata, "doc_genuineness_mode", "") == "verify" {
		exportPath, err = exportDocGenuinenessVerifyEnrichedCSV(ref, cleanRef)
	} else {
		exportPath, err = exportDocGenuinenessEnrichedCSV(ref, cleanRef)
	}
	if err != nil {
		return "", "", err
	}
	return exportPath, deriveDownloadFilename(ref, "doc_genuineness.csv"), nil
}

// ResolveClauseLabelDownload — clause_label jsonl artifact를 CSV로 변환.
// silverone 2026-06-01 (다운로드 컬럼 확장) — cleaned.parquet + doc_genuineness
// .jsonl LEFT JOIN으로 cleaned_text / raw_text / created_at / source_row_index
// / genuineness를 함께 포함. 어느 한쪽 artifact가 없거나 join이 실패해도
// row는 유지되고 해당 컬럼만 빈 값. exportClauseLabelEnrichedCSV 참조.
func (s *DatasetService) ResolveClauseLabelDownload(projectID, datasetID, datasetVersionID string) (string, string, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return "", "", err
	}
	ref := strings.TrimSpace(metadataString(version.Metadata, "clause_label_ref", ""))
	if ref == "" {
		ref = strings.TrimSpace(metadataString(version.Metadata, "clause_label_uri", ""))
	}
	if ref == "" {
		return "", "", ErrNotFound{Resource: "clause_label artifact"}
	}
	// silverone 2026-06-04 — CSV 변환 전에 jsonl이 읽을 수 있는지 검증.
	if err := validateArtifactReadable("clause_label", ref, artifactJSONL); err != nil {
		return "", "", err
	}
	cleanRef := strings.TrimSpace(metadataString(version.Metadata, "cleaned_ref", ""))
	if cleanRef == "" {
		cleanRef = strings.TrimSpace(metadataString(version.Metadata, "clean_uri", ""))
	}
	// verify(교차모델, ADR-028) 모드는 schema가 달라(검토 큐 필드 + model A/B/judge)
	// 전용 exporter를 쓴다. 단일 모델 경로는 dg genuineness join 포함 그대로 유지.
	if metadataString(version.Metadata, "clause_label_mode", "") == "verify" {
		exportPath, err := exportClauseLabelVerifyEnrichedCSV(ref, cleanRef)
		if err != nil {
			return "", "", err
		}
		return exportPath, deriveDownloadFilename(ref, "clause_label.csv"), nil
	}
	dgRef := strings.TrimSpace(metadataString(version.Metadata, "doc_genuineness_ref", ""))
	if dgRef == "" {
		dgRef = strings.TrimSpace(metadataString(version.Metadata, "doc_genuineness_uri", ""))
	}
	exportPath, err := exportClauseLabelEnrichedCSV(ref, cleanRef, dgRef)
	if err != nil {
		return "", "", err
	}
	return exportPath, deriveDownloadFilename(ref, "clause_label.csv"), nil
}

// deriveDownloadFilename — 원본 파일명(예: `xxx.jsonl`)을 `xxx.csv`로 치환한
// 뒤 KST 타임스탬프를 덧붙인다 (`xxx_20260601_134523.csv`). 빈 케이스는
// fallback 사용. doc_genuineness / clause_label 공통.
func deriveDownloadFilename(ref string, fallback string) string {
	filename := strings.TrimSpace(filepath.Base(ref))
	if filename == "" || filename == "." || filename == string(filepath.Separator) {
		return appendDownloadTimestamp(fallback)
	}
	lower := strings.ToLower(filename)
	switch {
	case strings.HasSuffix(lower, ".jsonl"):
		filename = filename[:len(filename)-len(".jsonl")] + ".csv"
	case strings.HasSuffix(lower, ".parquet"):
		filename = filename[:len(filename)-len(".parquet")] + ".csv"
	default:
		filename = filename + ".csv"
	}
	return appendDownloadTimestamp(filename)
}
