package service

import (
	"encoding/json"
	"os"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

type datasetProfileRegistry struct {
	Defaults map[string]string                `json:"defaults"`
	Profiles map[string]domain.DatasetProfile `json:"profiles"`
}

func loadDatasetProfileRegistry(path string) (*datasetProfileRegistry, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nil, nil
	}
	content, err := os.ReadFile(trimmed)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var registry datasetProfileRegistry
	if err := json.Unmarshal(content, &registry); err != nil {
		return nil, err
	}
	if len(registry.Defaults) == 0 && len(registry.Profiles) == 0 {
		return nil, nil
	}
	return &registry, nil
}

func (r *datasetProfileRegistry) resolve(dataType string, explicit *domain.DatasetProfile) *domain.DatasetProfile {
	explicit = normalizeDatasetProfile(explicit)
	if r == nil {
		return explicit
	}

	var base *domain.DatasetProfile
	if explicit != nil && strings.TrimSpace(explicit.ProfileID) != "" {
		base = r.profileByID(explicit.ProfileID)
	} else {
		base = r.defaultProfileForDataType(dataType)
	}

	if base == nil {
		return explicit
	}
	if explicit == nil {
		return cloneDatasetProfile(base)
	}
	return mergeDatasetProfiles(base, explicit)
}

func (r *datasetProfileRegistry) defaultProfileForDataType(dataType string) *domain.DatasetProfile {
	if r == nil {
		return nil
	}
	profileID := strings.TrimSpace(r.Defaults[strings.TrimSpace(dataType)])
	if profileID == "" {
		return nil
	}
	return r.profileByID(profileID)
}

func (r *datasetProfileRegistry) profileByID(profileID string) *domain.DatasetProfile {
	if r == nil {
		return nil
	}
	profile, ok := r.Profiles[strings.TrimSpace(profileID)]
	if !ok {
		return nil
	}
	if strings.TrimSpace(profile.ProfileID) == "" {
		profile.ProfileID = strings.TrimSpace(profileID)
	}
	return normalizeDatasetProfile(&profile)
}

func mergeDatasetProfiles(base, override *domain.DatasetProfile) *domain.DatasetProfile {
	base = normalizeDatasetProfile(base)
	override = normalizeDatasetProfile(override)
	if base == nil {
		return cloneDatasetProfile(override)
	}
	if override == nil {
		return cloneDatasetProfile(base)
	}
	merged := cloneDatasetProfile(base)
	if strings.TrimSpace(override.ProfileID) != "" {
		merged.ProfileID = strings.TrimSpace(override.ProfileID)
	}
	if override.PreparePromptVersion != nil {
		value := strings.TrimSpace(*override.PreparePromptVersion)
		merged.PreparePromptVersion = &value
	}
	if override.SentimentPromptVersion != nil {
		value := strings.TrimSpace(*override.SentimentPromptVersion)
		merged.SentimentPromptVersion = &value
	}
	if len(override.RegexRuleNames) > 0 {
		merged.RegexRuleNames = append([]string(nil), override.RegexRuleNames...)
	}
	if len(override.GarbageRuleNames) > 0 {
		merged.GarbageRuleNames = append([]string(nil), override.GarbageRuleNames...)
	}
	if override.EmbeddingModel != nil {
		value := strings.TrimSpace(*override.EmbeddingModel)
		merged.EmbeddingModel = &value
	}
	return normalizeDatasetProfile(merged)
}
