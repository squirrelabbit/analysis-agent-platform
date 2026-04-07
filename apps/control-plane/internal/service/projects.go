package service

import (
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"
)

type ProjectService struct {
	store store.Repository
}

func NewProjectService(repository store.Repository) *ProjectService {
	return &ProjectService{store: repository}
}

func (s *ProjectService) CreateProject(input domain.ProjectCreateRequest) (domain.Project, error) {
	name := strings.TrimSpace(input.Name)
	if name == "" {
		return domain.Project{}, ErrInvalidArgument{Message: "name is required"}
	}

	project := domain.Project{
		ProjectID:   id.New(),
		Name:        name,
		Description: input.Description,
		CreatedAt:   time.Now().UTC(),
	}
	if err := s.store.SaveProject(project); err != nil {
		return domain.Project{}, err
	}
	return project, nil
}

func (s *ProjectService) GetProject(projectID string) (domain.Project, error) {
	project, err := s.store.GetProject(projectID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Project{}, ErrNotFound{Resource: "project"}
		}
		return domain.Project{}, err
	}
	return project, nil
}

func (s *ProjectService) ListProjects() (domain.ProjectListResponse, error) {
	items, err := s.store.ListProjects()
	if err != nil {
		return domain.ProjectListResponse{}, err
	}
	return domain.ProjectListResponse{Items: items}, nil
}
