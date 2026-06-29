package service

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

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

// datasetArtifactPathOrFallback — artifactRoot 미설정 환경 fallback. 5/7 결정
// 5-step pipeline 신규 4 build skill에서 활용.
func (s *DatasetService) datasetArtifactPathOrFallback(version domain.DatasetVersion, scope, filename string) string {
	if path, ok := s.datasetArtifactPath(version, scope, filename); ok {
		return path
	}
	return strings.TrimSpace(version.StorageURI) + "." + filename
}

// removeArtifactFileQuietly — 빌드 중단 시 worker가 남긴 부분 artifact 파일을 지운다
// (silverone 2026-06-29). 중단은 결과를 저장하지 않으므로(재실행=처음부터) 파일도
// 남기면 안 된다 — metadata ref만 지우면 재실행이 같은 deterministic 경로로 ref를
// 다시 잡아 stale 부분본이 노출됐다. 없는 파일/빈 경로는 무시.
func removeArtifactFileQuietly(path string) {
	path = strings.TrimSpace(path)
	if path == "" {
		return
	}
	_ = os.Remove(path)
}

func (s *DatasetService) removeDatasetArtifacts(projectID, datasetID string) error {
	roots := []string{s.uploadRoot, s.artifactRoot}
	for _, root := range roots {
		if strings.TrimSpace(root) == "" {
			continue
		}
		target := filepath.Join(root, "projects", projectID, "datasets", datasetID)
		if err := os.RemoveAll(target); err != nil {
			return err
		}
	}
	return nil
}

func (s *DatasetService) removeDatasetVersionArtifacts(projectID, datasetID, datasetVersionID string) error {
	roots := []string{s.uploadRoot, s.artifactRoot}
	for _, root := range roots {
		if strings.TrimSpace(root) == "" {
			continue
		}
		target := filepath.Join(root, "projects", projectID, "datasets", datasetID, "versions", datasetVersionID)
		if err := os.RemoveAll(target); err != nil {
			return err
		}
	}
	return nil
}

func (s *DatasetService) isDatasetUploadSourcePath(projectID, datasetID, datasetVersionID, candidate string) bool {
	root := strings.TrimSpace(s.uploadRoot)
	if root == "" {
		return false
	}
	sourceRoot, err := filepath.Abs(filepath.Join(root, "projects", projectID, "datasets", datasetID, "versions", datasetVersionID, "source"))
	if err != nil {
		return false
	}
	relative, err := filepath.Rel(sourceRoot, candidate)
	if err != nil {
		return false
	}
	return relative == "." || (relative != "" && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}
