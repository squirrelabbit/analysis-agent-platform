package service

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"
)

type ProjectService struct {
	store        store.Repository
	uploadRoot   string
	artifactRoot string
}

func NewProjectService(repository store.Repository, uploadRoot string, artifactRoot string) *ProjectService {
	return &ProjectService{
		store:        repository,
		uploadRoot:   strings.TrimSpace(uploadRoot),
		artifactRoot: strings.TrimSpace(artifactRoot),
	}
}

func (s *ProjectService) CreateProject(input domain.ProjectCreateRequest) (domain.Project, error) {
	name := strings.TrimSpace(input.Name)
	if name == "" {
		return domain.Project{}, ErrInvalidArgument{Message: "name is required"}
	}
	// #31 — 프로젝트 생성 시 축제 메타(metadata.festival)를 함께 받을 수 있다.
	metadata, err := normalizeProjectMetadata(input.Metadata)
	if err != nil {
		return domain.Project{}, err
	}

	project := domain.Project{
		ProjectID:   id.New(),
		Name:        name,
		Description: input.Description,
		CreatedAt:   time.Now().UTC(),
		Metadata:    metadata,
	}
	if err := s.store.SaveProject(project); err != nil {
		return domain.Project{}, err
	}
	return project, nil
}

// UpdateProject — PATCH /projects/{pid}. non-nil 필드만 반영. metadata는 patch 병합
// (기존 key 유지 + 겹치면 덮어씀). festival을 건드릴 때만 재검증. 축제 메타 수정 경로.
func (s *ProjectService) UpdateProject(projectID string, req domain.ProjectUpdateRequest) (domain.Project, error) {
	project, err := s.store.GetProject(projectID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Project{}, ErrNotFound{Resource: "project"}
		}
		return domain.Project{}, err
	}
	if req.Name != nil {
		name := strings.TrimSpace(*req.Name)
		if name == "" {
			return domain.Project{}, ErrInvalidArgument{Message: "name must not be empty"}
		}
		project.Name = name
	}
	if req.Description != nil {
		project.Description = req.Description
	}
	if req.Metadata != nil {
		merged := map[string]any{}
		for k, v := range project.Metadata {
			merged[k] = v
		}
		for k, v := range req.Metadata {
			merged[k] = v
		}
		normalized, err := normalizeProjectMetadata(merged)
		if err != nil {
			return domain.Project{}, err
		}
		project.Metadata = normalized
	}
	if err := s.store.SaveProject(project); err != nil {
		return domain.Project{}, err
	}
	return s.withProjectCounts(project)
}

func (s *ProjectService) GetProject(projectID string) (domain.Project, error) {
	project, err := s.store.GetProject(projectID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Project{}, ErrNotFound{Resource: "project"}
		}
		return domain.Project{}, err
	}
	return s.withProjectCounts(project)
}

func (s *ProjectService) ListProjects() (domain.ProjectListResponse, error) {
	items, err := s.store.ListProjects()
	if err != nil {
		return domain.ProjectListResponse{}, err
	}
	for i := range items {
		items[i], err = s.withProjectCounts(items[i])
		if err != nil {
			return domain.ProjectListResponse{}, err
		}
	}
	return domain.ProjectListResponse{Items: items}, nil
}

func (s *ProjectService) DeleteProject(projectID string) error {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "project"}
		}
		return err
	}
	if err := s.store.DeleteProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "project"}
		}
		return err
	}
	if err := s.removeProjectArtifacts(projectID); err != nil {
		return err
	}
	return nil
}

func (s *ProjectService) withProjectCounts(project domain.Project) (domain.Project, error) {
	prompts, err := s.store.ListProjectPrompts(project.ProjectID)
	if err != nil {
		return domain.Project{}, err
	}
	datasets, err := s.store.ListDatasets(project.ProjectID)
	if err != nil {
		return domain.Project{}, err
	}

	datasetVersionCount := 0
	for _, dataset := range datasets {
		versions, err := s.store.ListDatasetVersions(project.ProjectID, dataset.DatasetID)
		if err != nil {
			return domain.Project{}, err
		}
		datasetVersionCount += len(versions)
	}

	// silverone 2026-06-01 — project 사이드바 채팅 count. dataset별 N+1 회피
	// 위해 단일 COUNT 쿼리. dataset 정책(archive/delete)이 없으므로 모든 thread
	// 합산 — dataset_version_count와 동일 기준.
	threadCount, err := s.store.CountAnalysisThreadsByProject(project.ProjectID)
	if err != nil {
		return domain.Project{}, err
	}

	project.DatasetCount = len(datasets)
	project.DatasetVersionCount = datasetVersionCount
	project.PromptCount = len(prompts)
	project.AnalysisThreadCount = threadCount
	return project, nil
}

func (s *ProjectService) removeProjectArtifacts(projectID string) error {
	roots := []string{s.uploadRoot, s.artifactRoot}
	for _, root := range roots {
		if strings.TrimSpace(root) == "" {
			continue
		}
		target := filepath.Join(root, "projects", projectID)
		if err := os.RemoveAll(target); err != nil {
			return err
		}
	}
	return nil
}
