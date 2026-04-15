package store

import (
	"sort"
	"sync"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

type MemoryStore struct {
	mu             sync.RWMutex
	projects       map[string]domain.Project
	projectPrompts map[string]domain.ProjectPrompt
	promptDefaults map[string]domain.ProjectPromptDefaults
	scenarios      map[string]domain.Scenario
	datasets       map[string]domain.Dataset
	versions       map[string]domain.DatasetVersion
	buildJobs      map[string]domain.DatasetBuildJob
	requests       map[string]domain.AnalysisRequest
	plans          map[string]domain.PlanRecord
	executions     map[string]domain.ExecutionSummary
	reports        map[string]domain.ReportDraft
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		projects:       make(map[string]domain.Project),
		projectPrompts: make(map[string]domain.ProjectPrompt),
		promptDefaults: make(map[string]domain.ProjectPromptDefaults),
		scenarios:      make(map[string]domain.Scenario),
		datasets:       make(map[string]domain.Dataset),
		versions:       make(map[string]domain.DatasetVersion),
		buildJobs:      make(map[string]domain.DatasetBuildJob),
		requests:       make(map[string]domain.AnalysisRequest),
		plans:          make(map[string]domain.PlanRecord),
		executions:     make(map[string]domain.ExecutionSummary),
		reports:        make(map[string]domain.ReportDraft),
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

func (s *MemoryStore) ListProjects() ([]domain.Project, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	items := make([]domain.Project, 0, len(s.projects))
	for _, project := range s.projects {
		items = append(items, project)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].ProjectID < items[j].ProjectID
		}
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	return items, nil
}

func projectPromptKey(projectID, version, operation string) string {
	return projectID + "::" + version + "::" + operation
}

func (s *MemoryStore) SaveProjectPrompt(prompt domain.ProjectPrompt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.projectPrompts[projectPromptKey(prompt.ProjectID, prompt.Version, prompt.Operation)] = prompt
	return nil
}

func (s *MemoryStore) GetProjectPrompt(projectID, version, operation string) (domain.ProjectPrompt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prompt, ok := s.projectPrompts[projectPromptKey(projectID, version, operation)]
	if !ok {
		return domain.ProjectPrompt{}, ErrNotFound
	}
	return prompt, nil
}

func (s *MemoryStore) ListProjectPrompts(projectID string) ([]domain.ProjectPrompt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.ProjectPrompt, 0)
	for _, prompt := range s.projectPrompts {
		if prompt.ProjectID != projectID {
			continue
		}
		items = append(items, prompt)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Version == items[j].Version {
			if items[i].Operation == items[j].Operation {
				return items[i].UpdatedAt.After(items[j].UpdatedAt)
			}
			return items[i].Operation < items[j].Operation
		}
		return items[i].Version < items[j].Version
	})
	return items, nil
}

func (s *MemoryStore) SaveProjectPromptDefaults(defaults domain.ProjectPromptDefaults) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.promptDefaults[defaults.ProjectID] = defaults
	return nil
}

func (s *MemoryStore) GetProjectPromptDefaults(projectID string) (domain.ProjectPromptDefaults, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	defaults, ok := s.promptDefaults[projectID]
	if !ok {
		return domain.ProjectPromptDefaults{}, ErrNotFound
	}
	return defaults, nil
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

func (s *MemoryStore) ListDatasets(projectID string) ([]domain.Dataset, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.Dataset, 0)
	for _, dataset := range s.datasets {
		if dataset.ProjectID != projectID {
			continue
		}
		items = append(items, dataset)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].DatasetID < items[j].DatasetID
		}
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	return items, nil
}

func (s *MemoryStore) SaveDatasetVersion(version domain.DatasetVersion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versions[version.DatasetVersionID] = cloneDatasetVersion(version)
	return nil
}

func (s *MemoryStore) GetDatasetVersion(projectID, datasetVersionID string) (domain.DatasetVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	version, ok := s.versions[datasetVersionID]
	if !ok || version.ProjectID != projectID {
		return domain.DatasetVersion{}, ErrNotFound
	}
	return cloneDatasetVersion(version), nil
}

func (s *MemoryStore) ListDatasetVersions(projectID, datasetID string) ([]domain.DatasetVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.DatasetVersion, 0)
	for _, version := range s.versions {
		if version.ProjectID != projectID {
			continue
		}
		if datasetID != "" && version.DatasetID != datasetID {
			continue
		}
		items = append(items, cloneDatasetVersion(version))
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].DatasetVersionID > items[j].DatasetVersionID
		}
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
	return items, nil
}

func cloneDatasetVersion(version domain.DatasetVersion) domain.DatasetVersion {
	cloned := version
	cloned.Metadata = cloneAnyMap(version.Metadata)
	if version.Profile != nil {
		profile := *version.Profile
		profile.RegexRuleNames = append([]string(nil), version.Profile.RegexRuleNames...)
		profile.GarbageRuleNames = append([]string(nil), version.Profile.GarbageRuleNames...)
		cloned.Profile = &profile
	}
	if version.PrepareSummary != nil {
		summary := *version.PrepareSummary
		if len(version.PrepareSummary.PrepareRegexRuleHits) > 0 {
			summary.PrepareRegexRuleHits = make(map[string]int, len(version.PrepareSummary.PrepareRegexRuleHits))
			for key, value := range version.PrepareSummary.PrepareRegexRuleHits {
				summary.PrepareRegexRuleHits[key] = value
			}
		}
		cloned.PrepareSummary = &summary
	}
	return cloned
}

func cloneAnyMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = cloneAnyValue(value)
	}
	return output
}

func cloneAnyValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneAnyMap(typed)
	case []any:
		cloned := make([]any, len(typed))
		for index, item := range typed {
			cloned[index] = cloneAnyValue(item)
		}
		return cloned
	case []string:
		return append([]string(nil), typed...)
	case []int:
		return append([]int(nil), typed...)
	case map[string]string:
		cloned := make(map[string]string, len(typed))
		for key, item := range typed {
			cloned[key] = item
		}
		return cloned
	case map[string]int:
		cloned := make(map[string]int, len(typed))
		for key, item := range typed {
			cloned[key] = item
		}
		return cloned
	default:
		return value
	}
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
