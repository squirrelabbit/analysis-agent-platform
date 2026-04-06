package store

import (
	"sort"
	"sync"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

type MemoryStore struct {
	mu         sync.RWMutex
	projects   map[string]domain.Project
	scenarios  map[string]domain.Scenario
	datasets   map[string]domain.Dataset
	versions   map[string]domain.DatasetVersion
	buildJobs  map[string]domain.DatasetBuildJob
	requests   map[string]domain.AnalysisRequest
	plans      map[string]domain.PlanRecord
	executions map[string]domain.ExecutionSummary
	reports    map[string]domain.ReportDraft
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		projects:   make(map[string]domain.Project),
		scenarios:  make(map[string]domain.Scenario),
		datasets:   make(map[string]domain.Dataset),
		versions:   make(map[string]domain.DatasetVersion),
		buildJobs:  make(map[string]domain.DatasetBuildJob),
		requests:   make(map[string]domain.AnalysisRequest),
		plans:      make(map[string]domain.PlanRecord),
		executions: make(map[string]domain.ExecutionSummary),
		reports:    make(map[string]domain.ReportDraft),
	}
}

func (s *MemoryStore) SaveProject(project domain.Project) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.projects[project.ProjectID] = project
	return nil
}

func scenarioKey(projectID, scenarioID string) string {
	return projectID + "::" + scenarioID
}

func (s *MemoryStore) GetProject(projectID string) (domain.Project, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	project, ok := s.projects[projectID]
	if !ok {
		return domain.Project{}, ErrNotFound
	}
	return project, nil
}

func (s *MemoryStore) SaveScenario(scenario domain.Scenario) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scenarios[scenarioKey(scenario.ProjectID, scenario.ScenarioID)] = scenario
	return nil
}

func (s *MemoryStore) GetScenario(projectID, scenarioID string) (domain.Scenario, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	scenario, ok := s.scenarios[scenarioKey(projectID, scenarioID)]
	if !ok {
		return domain.Scenario{}, ErrNotFound
	}
	return scenario, nil
}

func (s *MemoryStore) ListScenarios(projectID string) ([]domain.Scenario, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.Scenario, 0)
	for _, scenario := range s.scenarios {
		if scenario.ProjectID != projectID {
			continue
		}
		items = append(items, scenario)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].ScenarioID < items[j].ScenarioID
		}
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	return items, nil
}

func (s *MemoryStore) SaveDataset(dataset domain.Dataset) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.datasets[dataset.DatasetID] = dataset
	return nil
}

func (s *MemoryStore) GetDataset(projectID, datasetID string) (domain.Dataset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dataset, ok := s.datasets[datasetID]
	if !ok || dataset.ProjectID != projectID {
		return domain.Dataset{}, ErrNotFound
	}
	return dataset, nil
}

func (s *MemoryStore) SaveDatasetVersion(version domain.DatasetVersion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versions[version.DatasetVersionID] = version
	return nil
}

func (s *MemoryStore) GetDatasetVersion(projectID, datasetVersionID string) (domain.DatasetVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	version, ok := s.versions[datasetVersionID]
	if !ok || version.ProjectID != projectID {
		return domain.DatasetVersion{}, ErrNotFound
	}
	return version, nil
}

func (s *MemoryStore) SaveDatasetBuildJob(job domain.DatasetBuildJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.buildJobs[job.JobID]; ok && job.CreatedAt.IsZero() {
		job.CreatedAt = existing.CreatedAt
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now().UTC()
	}
	s.buildJobs[job.JobID] = job
	return nil
}

func (s *MemoryStore) GetDatasetBuildJob(projectID, jobID string) (domain.DatasetBuildJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.buildJobs[jobID]
	if !ok || job.ProjectID != projectID {
		return domain.DatasetBuildJob{}, ErrNotFound
	}
	return job, nil
}

func (s *MemoryStore) ListDatasetBuildJobs(projectID, datasetVersionID string) ([]domain.DatasetBuildJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]domain.DatasetBuildJob, 0)
	for _, job := range s.buildJobs {
		if job.ProjectID != projectID {
			continue
		}
		if datasetVersionID != "" && job.DatasetVersionID != datasetVersionID {
			continue
		}
		items = append(items, job)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].JobID > items[j].JobID
		}
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
	return items, nil
}

func (s *MemoryStore) SaveRequest(request domain.AnalysisRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests[request.RequestID] = request
	return nil
}

func (s *MemoryStore) GetRequest(projectID, requestID string) (domain.AnalysisRequest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	request, ok := s.requests[requestID]
	if !ok || request.ProjectID != projectID {
		return domain.AnalysisRequest{}, ErrNotFound
	}
	return request, nil
}

func (s *MemoryStore) SavePlan(plan domain.PlanRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.plans[plan.PlanID] = plan
	return nil
}

func (s *MemoryStore) GetPlan(projectID, planID string) (domain.PlanRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	plan, ok := s.plans[planID]
	if !ok || plan.ProjectID != projectID {
		return domain.PlanRecord{}, ErrNotFound
	}
	return plan, nil
}

func (s *MemoryStore) SaveExecution(execution domain.ExecutionSummary) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.executions[execution.ExecutionID]; ok && execution.CreatedAt.IsZero() {
		execution.CreatedAt = existing.CreatedAt
	}
	if execution.CreatedAt.IsZero() {
		execution.CreatedAt = time.Now().UTC()
	}
	s.executions[execution.ExecutionID] = execution
	return nil
}

func (s *MemoryStore) GetExecution(projectID, executionID string) (domain.ExecutionSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	execution, ok := s.executions[executionID]
	if !ok || execution.ProjectID != projectID {
		return domain.ExecutionSummary{}, ErrNotFound
	}
	return execution, nil
}

func (s *MemoryStore) ListExecutions(projectID string) ([]domain.ExecutionSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]domain.ExecutionSummary, 0)
	for _, execution := range s.executions {
		if execution.ProjectID != projectID {
			continue
		}
		items = append(items, execution)
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
	return items, nil
}

func (s *MemoryStore) SaveReportDraft(draft domain.ReportDraft) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reports[draft.DraftID] = draft
	return nil
}

func (s *MemoryStore) GetReportDraft(projectID, draftID string) (domain.ReportDraft, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	draft, ok := s.reports[draftID]
	if !ok || draft.ProjectID != projectID {
		return domain.ReportDraft{}, ErrNotFound
	}
	return draft, nil
}
