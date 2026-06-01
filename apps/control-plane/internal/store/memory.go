package store

import (
	"errors"
	"sort"
	"sync"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
)

// errInvalidStoreInput은 store 호출에서 입력 invariant 위반 시 사용.
// service layer가 ErrInvalidArgument로 wrapping하기 전 단계 — store는
// 직접 contract 위반만 보고하면 된다.
func errInvalidStoreInput(message string) error {
	return errors.New("store: " + message)
}

type MemoryStore struct {
	mu                   sync.RWMutex
	projects             map[string]domain.Project
	projectPrompts       map[string]domain.ProjectPrompt
	promptDefaults       map[string]domain.ProjectPromptDefaults
	projectPromptChanges []domain.ProjectPromptChange // append-only, ordered
	datasets             map[string]domain.Dataset
	versions             map[string]domain.DatasetVersion
	artifacts            map[string]domain.DatasetVersionArtifact
	buildJobs            map[string]domain.DatasetBuildJob
	analysisThreads      map[string]domain.AnalysisThread
	analysisMessages     map[string]domain.AnalysisMessage
	analysisRuns         map[string]domain.AnalysisRun
	// document_cluster_profile build / confirmation 관련 필드는 β2 (5/19) 결정으로 제거.
	// scenarios / requests / plans / executions / reports 필드는 δ-3 (5/21) plan_v2 도입에 따라 제거.
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		projects:             make(map[string]domain.Project),
		projectPrompts:       make(map[string]domain.ProjectPrompt),
		promptDefaults:       make(map[string]domain.ProjectPromptDefaults),
		projectPromptChanges: nil,
		datasets:             make(map[string]domain.Dataset),
		versions:             make(map[string]domain.DatasetVersion),
		artifacts:            make(map[string]domain.DatasetVersionArtifact),
		buildJobs:            make(map[string]domain.DatasetBuildJob),
		analysisThreads:      make(map[string]domain.AnalysisThread),
		analysisMessages:     make(map[string]domain.AnalysisMessage),
		analysisRuns:         make(map[string]domain.AnalysisRun),
	}
}

func (s *MemoryStore) SaveProject(project domain.Project) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.projects[project.ProjectID] = project
	return nil
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

func (s *MemoryStore) DeleteProject(projectID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.projects[projectID]; !ok {
		return ErrNotFound
	}

	delete(s.projects, projectID)
	delete(s.promptDefaults, projectID)

	for key, prompt := range s.projectPrompts {
		if prompt.ProjectID == projectID {
			delete(s.projectPrompts, key)
		}
	}
	for key, dataset := range s.datasets {
		if dataset.ProjectID == projectID {
			delete(s.datasets, key)
		}
	}
	for key, version := range s.versions {
		if version.ProjectID == projectID {
			delete(s.versions, key)
		}
	}
	for key, artifact := range s.artifacts {
		if artifact.ProjectID == projectID {
			delete(s.artifacts, key)
		}
	}
	for key, job := range s.buildJobs {
		if job.ProjectID == projectID {
			delete(s.buildJobs, key)
		}
	}
	for key, thread := range s.analysisThreads {
		if thread.ProjectID == projectID {
			delete(s.analysisThreads, key)
		}
	}
	for key, message := range s.analysisMessages {
		if message.ProjectID == projectID {
			delete(s.analysisMessages, key)
		}
	}
	for key, run := range s.analysisRuns {
		if run.ProjectID == projectID {
			delete(s.analysisRuns, key)
		}
	}
	return nil
}

// 5/6 화면기획서 B안 채택: 전역 prompts 테이블 폐기. SavePrompt/GetPrompt/
// GetPromptByVersion/ListPrompts/DeletePrompt 5개 + promptVersionKey 헬퍼
// 모두 제거. 글로벌 prompt는 .md 코드 계약. project_prompts만 유지.

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

// AppendProjectPromptChange appends a single audit log row.
// ADR-015 §C: append-only — caller never updates existing rows.
func (s *MemoryStore) AppendProjectPromptChange(change domain.ProjectPromptChange) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.projectPromptChanges = append(s.projectPromptChanges, change)
	return nil
}

// ListProjectPromptChanges returns the audit log filtered by project (and
// optionally operation), oldest-first. An empty operation returns every
// project change so the API layer can paginate as needed.
func (s *MemoryStore) ListProjectPromptChanges(projectID, operation string) ([]domain.ProjectPromptChange, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.ProjectPromptChange, 0)
	for _, change := range s.projectPromptChanges {
		if change.ProjectID != projectID {
			continue
		}
		if operation != "" && change.Operation != operation {
			continue
		}
		out = append(out, change)
	}
	return out, nil
}

// 5/7 결정: rule/taxonomy/stopwords를 DB 기반으로 관리. 자산별 7 메서드 ×
// 3 자산 = 21 메서드. PK는 (project_id, version). prompt 정책의 패턴을
// 그대로 답습 — defaults는 1 프로젝트당 active 1개, changes는 append-only.

func projectAssetKey(projectID, version string) string {
	return projectID + "::" + version
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

func (s *MemoryStore) DeleteDataset(projectID, datasetID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dataset, ok := s.datasets[datasetID]
	if !ok || dataset.ProjectID != projectID {
		return ErrNotFound
	}

	delete(s.datasets, datasetID)
	for key, version := range s.versions {
		if version.ProjectID == projectID && version.DatasetID == datasetID {
			delete(s.versions, key)
		}
	}
	for key, artifact := range s.artifacts {
		if artifact.ProjectID == projectID && artifact.DatasetID == datasetID {
			delete(s.artifacts, key)
		}
	}
	for key, job := range s.buildJobs {
		if job.ProjectID == projectID && job.DatasetID == datasetID {
			delete(s.buildJobs, key)
		}
	}
	for key, thread := range s.analysisThreads {
		if thread.ProjectID == projectID && thread.DatasetID == datasetID {
			delete(s.analysisThreads, key)
		}
	}
	for key, message := range s.analysisMessages {
		if message.ProjectID == projectID && message.DatasetID == datasetID {
			delete(s.analysisMessages, key)
		}
	}
	for key, run := range s.analysisRuns {
		if run.ProjectID == projectID && run.DatasetID == datasetID {
			delete(s.analysisRuns, key)
		}
	}
	return nil
}

func (s *MemoryStore) SaveDatasetVersion(version domain.DatasetVersion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	version = normalizeDatasetVersionCleanFields(version)
	s.versions[version.DatasetVersionID] = cloneDatasetVersion(version)
	s.syncDatasetVersionArtifactsLocked(version)
	return nil
}

func (s *MemoryStore) GetDatasetVersion(projectID, datasetVersionID string) (domain.DatasetVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	version, ok := s.versions[datasetVersionID]
	if !ok || version.ProjectID != projectID {
		return domain.DatasetVersion{}, ErrNotFound
	}
	cloned := cloneDatasetVersion(version)
	cloned.Artifacts = s.datasetVersionArtifactsLocked(projectID, datasetVersionID)
	return cloned, nil
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
		cloned := cloneDatasetVersion(version)
		cloned.Artifacts = s.datasetVersionArtifactsLocked(projectID, version.DatasetVersionID)
		items = append(items, cloned)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].DatasetVersionID > items[j].DatasetVersionID
		}
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
	return items, nil
}

func (s *MemoryStore) DeleteDatasetVersion(projectID, datasetID, datasetVersionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	version, ok := s.versions[datasetVersionID]
	if !ok || version.ProjectID != projectID || version.DatasetID != datasetID {
		return ErrNotFound
	}

	delete(s.versions, datasetVersionID)
	for key, artifact := range s.artifacts {
		if artifact.ProjectID == projectID && artifact.DatasetID == datasetID && artifact.DatasetVersionID == datasetVersionID {
			delete(s.artifacts, key)
		}
	}
	for key, job := range s.buildJobs {
		if job.ProjectID == projectID && job.DatasetID == datasetID && job.DatasetVersionID == datasetVersionID {
			delete(s.buildJobs, key)
		}
	}
	if dataset, ok := s.datasets[datasetID]; ok && dataset.ProjectID == projectID && dataset.ActiveDatasetVersionID != nil && *dataset.ActiveDatasetVersionID == datasetVersionID {
		now := time.Now().UTC()
		dataset.ActiveDatasetVersionID = nil
		dataset.ActiveVersionUpdatedAt = &now
		s.datasets[datasetID] = dataset
	}
	return nil
}

func (s *MemoryStore) ListDatasetVersionArtifacts(projectID, datasetVersionID string) ([]domain.DatasetVersionArtifact, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.datasetVersionArtifactsLocked(projectID, datasetVersionID), nil
}

func (s *MemoryStore) syncDatasetVersionArtifactsLocked(version domain.DatasetVersion) {
	now := time.Now().UTC()
	// silverone 2026-05-28 (B1): no-op update 방지. 기존 row와 payload field가
	// 모두 동일하면 UpdatedAt을 그대로 유지한다. postgres store의 `ON CONFLICT
	// DO UPDATE ... WHERE` SQL과 같은 의미. GET dataset_version 흐름이 본
	// 함수를 호출해도 값이 같으면 row가 touch되지 않는다.
	existingByType := make(map[string]domain.DatasetVersionArtifact)
	keyByType := make(map[string]string)
	for key, artifact := range s.artifacts {
		if artifact.DatasetVersionID != version.DatasetVersionID {
			continue
		}
		existingByType[artifact.ArtifactType] = artifact
		keyByType[artifact.ArtifactType] = key
	}

	derived := deriveDatasetVersionArtifacts(version, now)
	derivedTypes := make(map[string]struct{}, len(derived))
	for _, next := range derived {
		derivedTypes[next.ArtifactType] = struct{}{}
		if prev, ok := existingByType[next.ArtifactType]; ok {
			if !prev.CreatedAt.IsZero() {
				next.CreatedAt = prev.CreatedAt
			}
			if datasetVersionArtifactPayloadEqual(prev, next) {
				next.UpdatedAt = prev.UpdatedAt
			}
		}
		s.artifacts[next.ArtifactID] = cloneDatasetVersionArtifact(next)
	}
	// derive에 없는 옛 artifact_type은 stale — 제거.
	for artifactType, key := range keyByType {
		if _, kept := derivedTypes[artifactType]; !kept {
			delete(s.artifacts, key)
		}
	}
}

func (s *MemoryStore) datasetVersionArtifactsLocked(projectID, datasetVersionID string) []domain.DatasetVersionArtifact {
	items := make([]domain.DatasetVersionArtifact, 0)
	for _, artifact := range s.artifacts {
		if artifact.ProjectID != projectID || artifact.DatasetVersionID != datasetVersionID {
			continue
		}
		items = append(items, cloneDatasetVersionArtifact(artifact))
	}
	sort.Slice(items, func(i, j int) bool {
		if artifactStageOrder(items[i].Stage) == artifactStageOrder(items[j].Stage) {
			return items[i].ArtifactType < items[j].ArtifactType
		}
		return artifactStageOrder(items[i].Stage) < artifactStageOrder(items[j].Stage)
	})
	return items
}

func cloneDatasetVersion(version domain.DatasetVersion) domain.DatasetVersion {
	cloned := version
	cloned.Metadata = cloneAnyMap(version.Metadata)
	cloned.SourceSummary = nil
	cloned.BuildJobs = nil
	cloned.BuildStages = nil
	cloned.Artifacts = cloneDatasetVersionArtifacts(version.Artifacts)
	if version.Profile != nil {
		profile := *version.Profile
		profile.RegexRuleNames = append([]string(nil), version.Profile.RegexRuleNames...)
		profile.GarbageRuleNames = append([]string(nil), version.Profile.GarbageRuleNames...)
		cloned.Profile = &profile
	}
	// silverone 2026-05-28 (β2 cleanup PR2) — PrepareSummary deep clone 제거.
	if version.CleanSummary != nil {
		summary := *version.CleanSummary
		if len(version.CleanSummary.TextColumns) > 0 {
			summary.TextColumns = append([]string(nil), version.CleanSummary.TextColumns...)
		}
		if len(version.CleanSummary.CleanRegexRuleHits) > 0 {
			summary.CleanRegexRuleHits = make(map[string]int, len(version.CleanSummary.CleanRegexRuleHits))
			for key, value := range version.CleanSummary.CleanRegexRuleHits {
				summary.CleanRegexRuleHits[key] = value
			}
		}
		cloned.CleanSummary = &summary
	}
	return cloned
}

func cloneDatasetVersionArtifacts(items []domain.DatasetVersionArtifact) []domain.DatasetVersionArtifact {
	if len(items) == 0 {
		return nil
	}
	cloned := make([]domain.DatasetVersionArtifact, 0, len(items))
	for _, item := range items {
		cloned = append(cloned, cloneDatasetVersionArtifact(item))
	}
	return cloned
}

func cloneDatasetVersionArtifact(artifact domain.DatasetVersionArtifact) domain.DatasetVersionArtifact {
	cloned := artifact
	cloned.Summary = cloneAnyMap(artifact.Summary)
	cloned.Metadata = cloneAnyMap(artifact.Metadata)
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

func (s *MemoryStore) SaveAnalysisThread(thread domain.AnalysisThread) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.analysisThreads[thread.ThreadID]; ok {
		if thread.CreatedAt.IsZero() {
			thread.CreatedAt = existing.CreatedAt
		}
	}
	if thread.CreatedAt.IsZero() {
		thread.CreatedAt = time.Now().UTC()
	}
	if thread.UpdatedAt.IsZero() {
		thread.UpdatedAt = thread.CreatedAt
	}
	s.analysisThreads[thread.ThreadID] = thread
	return nil
}

func (s *MemoryStore) GetAnalysisThread(projectID, datasetID, threadID string) (domain.AnalysisThread, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	thread, ok := s.analysisThreads[threadID]
	if !ok || thread.ProjectID != projectID || thread.DatasetID != datasetID {
		return domain.AnalysisThread{}, ErrNotFound
	}
	return s.withAnalysisThreadStatsLocked(thread), nil
}

func (s *MemoryStore) ListAnalysisThreads(projectID, datasetID string) ([]domain.AnalysisThread, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.AnalysisThread, 0)
	for _, thread := range s.analysisThreads {
		if thread.ProjectID != projectID || thread.DatasetID != datasetID {
			continue
		}
		items = append(items, s.withAnalysisThreadStatsLocked(thread))
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].UpdatedAt.Equal(items[j].UpdatedAt) {
			return items[i].ThreadID > items[j].ThreadID
		}
		return items[i].UpdatedAt.After(items[j].UpdatedAt)
	})
	return items, nil
}

// silverone 2026-06-01 — project 사이드바 채팅 count. dataset 무관 합산.
func (s *MemoryStore) CountAnalysisThreadsByProject(projectID string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, thread := range s.analysisThreads {
		if thread.ProjectID == projectID {
			count++
		}
	}
	return count, nil
}

func (s *MemoryStore) SaveAnalysisMessage(message domain.AnalysisMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if message.CreatedAt.IsZero() {
		message.CreatedAt = time.Now().UTC()
	}
	message.ContextSummary = cloneAnyMap(message.ContextSummary)
	s.analysisMessages[message.MessageID] = message
	if thread, ok := s.analysisThreads[message.ThreadID]; ok {
		thread.UpdatedAt = message.CreatedAt
		if thread.Title == "" && message.Role == "user" {
			thread.Title = truncateAnalysisTitle(message.Content)
		}
		s.analysisThreads[message.ThreadID] = thread
	}
	return nil
}

func (s *MemoryStore) ListAnalysisMessages(projectID, threadID string) ([]domain.AnalysisMessage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if thread, ok := s.analysisThreads[threadID]; !ok || thread.ProjectID != projectID {
		return nil, ErrNotFound
	}
	items := make([]domain.AnalysisMessage, 0)
	for _, message := range s.analysisMessages {
		if message.ProjectID != projectID || message.ThreadID != threadID {
			continue
		}
		cloned := message
		cloned.ContextSummary = cloneAnyMap(message.ContextSummary)
		items = append(items, cloned)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].CreatedAt.Equal(items[j].CreatedAt) {
			return items[i].MessageID < items[j].MessageID
		}
		return items[i].CreatedAt.Before(items[j].CreatedAt)
	})
	return items, nil
}

func (s *MemoryStore) SaveAnalysisRun(run domain.AnalysisRun) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.analysisRuns[run.RunID]; ok && run.CreatedAt.IsZero() {
		run.CreatedAt = existing.CreatedAt
	}
	if run.CreatedAt.IsZero() {
		run.CreatedAt = time.Now().UTC()
	}
	run.RequestJSON = cloneAnyMap(run.RequestJSON)
	run.ResultJSON = append([]byte(nil), run.ResultJSON...)
	s.analysisRuns[run.RunID] = run
	return nil
}

func (s *MemoryStore) GetAnalysisRun(projectID, runID string) (domain.AnalysisRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	run, ok := s.analysisRuns[runID]
	if !ok || run.ProjectID != projectID {
		return domain.AnalysisRun{}, ErrNotFound
	}
	run.RequestJSON = cloneAnyMap(run.RequestJSON)
	run.ResultJSON = append([]byte(nil), run.ResultJSON...)
	return run, nil
}

// GetLastSuccessfulAnalysisRun — silverone 2026-05-26 (plan reuse POC-1).
// thread 안 모든 run 중 status == "completed" 이고 가장 늦은 created_at을
// 가진 run을 반환한다. 없으면 ErrNotFound. tie-break은 RunID 사전순.
func (s *MemoryStore) GetLastSuccessfulAnalysisRun(projectID, threadID string) (domain.AnalysisRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var best *domain.AnalysisRun
	for runID, run := range s.analysisRuns {
		if run.ProjectID != projectID || run.ThreadID != threadID {
			continue
		}
		if run.Status != "completed" {
			continue
		}
		if best == nil ||
			run.CreatedAt.After(best.CreatedAt) ||
			(run.CreatedAt.Equal(best.CreatedAt) && runID > best.RunID) {
			r := run
			best = &r
		}
	}
	if best == nil {
		return domain.AnalysisRun{}, ErrNotFound
	}
	out := *best
	out.RequestJSON = cloneAnyMap(out.RequestJSON)
	out.ResultJSON = append([]byte(nil), out.ResultJSON...)
	return out, nil
}

// ListInFlightDatasetBuildJobs / ListInFlightAnalysisRuns —
// silverone 2026-05-27 (Codex adversarial review fix-2). startup reconciliation
// 에서 사용. 전체 system 단위로 미완료 row를 가져온다.
func (s *MemoryStore) ListInFlightDatasetBuildJobs() ([]domain.DatasetBuildJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.DatasetBuildJob, 0)
	for _, job := range s.buildJobs {
		if job.Status == "queued" || job.Status == "running" {
			items = append(items, job)
		}
	}
	return items, nil
}

func (s *MemoryStore) ListInFlightAnalysisRuns() ([]domain.AnalysisRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]domain.AnalysisRun, 0)
	for _, run := range s.analysisRuns {
		if run.Status == "running" {
			out := run
			out.RequestJSON = cloneAnyMap(out.RequestJSON)
			out.ResultJSON = append([]byte(nil), out.ResultJSON...)
			items = append(items, out)
		}
	}
	return items, nil
}

func (s *MemoryStore) withAnalysisThreadStatsLocked(thread domain.AnalysisThread) domain.AnalysisThread {
	thread.MessageCount = 0
	thread.LastMessage = ""
	var lastAt time.Time
	for _, message := range s.analysisMessages {
		if message.ThreadID != thread.ThreadID || message.ProjectID != thread.ProjectID {
			continue
		}
		thread.MessageCount++
		if message.CreatedAt.After(lastAt) || lastAt.IsZero() {
			lastAt = message.CreatedAt
			thread.LastMessage = truncateAnalysisTitle(message.Content)
		}
	}
	return thread
}

func truncateAnalysisTitle(value string) string {
	const maxRunes = 80
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return string(runes[:maxRunes])
}

// document_cluster_profile build / confirmation 관련 memory store method는
// β2 (5/19) 결정으로 제거.
