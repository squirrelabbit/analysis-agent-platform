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
