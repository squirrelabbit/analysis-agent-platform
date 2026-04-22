package service

import (
	"fmt"
	"sort"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

func (s *DatasetService) resolveDatasetProfile(dataType string, explicit *domain.DatasetProfile) *domain.DatasetProfile {
	return s.profileRegistry.resolve(dataType, explicit)
}

func (s *DatasetService) ValidateDatasetProfiles() (domain.DatasetProfileValidationResponse, error) {
	response := domain.DatasetProfileValidationResponse{
		Registry: domain.DatasetProfileRegistryView{
			SourcePath:         s.datasetProfilesPath,
			PromptTemplatesDir: s.promptTemplatesDir,
		},
		Valid: true,
	}
	if s.profileRegistry != nil {
		response.Registry.Defaults = cloneStringMap(s.profileRegistry.Defaults)
		response.Registry.Profiles = cloneProfileMap(s.profileRegistry.Profiles)
	}

	promptCatalog, err := s.promptCatalog()
	if err != nil {
		return domain.DatasetProfileValidationResponse{}, err
	}
	response.Registry.PromptCatalog = promptCatalog
	promptVersions := make([]string, 0, len(promptCatalog))
	promptMetadata := make(map[string]domain.PromptTemplateMetadata, len(promptCatalog))
	for _, item := range promptCatalog {
		promptVersions = append(promptVersions, strings.TrimSpace(item.Version))
		promptMetadata[strings.TrimSpace(item.Version)] = item
	}
	response.Registry.AvailablePromptVersions = promptVersions

	issues := make([]domain.DatasetProfileValidationIssue, 0)
	var ruleCatalog *domain.DatasetProfileRuleCatalog
	if capabilities, err := s.fetchWorkerCapabilities(); err != nil {
		issues = append(issues, domain.DatasetProfileValidationIssue{
			Severity: "warning",
			Code:     "worker_capabilities_unavailable",
			Message:  fmt.Sprintf("python-ai worker capability 조회에 실패했습니다: %v", err),
			Scope:    "worker",
		})
	} else if capabilities != nil {
		response.Registry.RuleCatalog = &capabilities.RuleCatalog
		ruleCatalog = &capabilities.RuleCatalog
		if len(response.Registry.PromptCatalog) == 0 && len(capabilities.PromptCatalog) > 0 {
			response.Registry.PromptCatalog = capabilities.PromptCatalog
			response.Registry.AvailablePromptVersions = nil
			promptMetadata = make(map[string]domain.PromptTemplateMetadata, len(capabilities.PromptCatalog))
			for _, item := range capabilities.PromptCatalog {
				version := strings.TrimSpace(item.Version)
				if version == "" {
					continue
				}
				response.Registry.AvailablePromptVersions = append(response.Registry.AvailablePromptVersions, version)
				promptMetadata[version] = item
			}
		}
	}
	availablePrepareRules := stringSet(nil)
	availableGarbageRules := stringSet(nil)
	if ruleCatalog != nil {
		availablePrepareRules = stringSet(ruleCatalog.AvailablePrepareRegexRuleNames)
		availableGarbageRules = stringSet(ruleCatalog.AvailableGarbageRuleNames)
	}
	validatePromptVersion := func(owner, scope, resourceRef, fieldName string, value *string, allowedOperations ...string) {
		trimmed := strings.TrimSpace(optionalStringValue(value))
		if trimmed == "" {
			return
		}
		meta, ok := promptMetadata[trimmed]
		if !ok {
			issues = append(issues, domain.DatasetProfileValidationIssue{
				Severity:    "error",
				Code:        fieldName + "_missing",
				Message:     fmt.Sprintf("%s 의 %s %q 가 prompt 디렉터리에 없습니다.", owner, fieldName, trimmed),
				Scope:       scope,
				ResourceRef: resourceRef,
			})
			return
		}
		operation := strings.TrimSpace(meta.Operation)
		if operation == "" {
			return
		}
		for _, allowed := range allowedOperations {
			if operation == allowed {
				return
			}
		}
		issues = append(issues, domain.DatasetProfileValidationIssue{
			Severity:    "error",
			Code:        fieldName + "_operation_mismatch",
			Message:     fmt.Sprintf("%s 의 %s %q 는 %s 작업용 prompt가 아닙니다.", owner, fieldName, trimmed, strings.Join(allowedOperations, "/")),
			Scope:       scope,
			ResourceRef: resourceRef,
		})
	}
	validateRuleNames := func(owner, scope, resourceRef, fieldName string, values []string, available map[string]struct{}) {
		if len(values) == 0 {
			return
		}
		if len(available) == 0 {
			issues = append(issues, domain.DatasetProfileValidationIssue{
				Severity:    "warning",
				Code:        fieldName + "_catalog_unavailable",
				Message:     fmt.Sprintf("%s 의 %s 를 검증할 worker rule catalog를 조회하지 못했습니다.", owner, fieldName),
				Scope:       scope,
				ResourceRef: resourceRef,
			})
			return
		}
		for _, value := range values {
			ruleName := strings.TrimSpace(value)
			if ruleName == "" {
				continue
			}
			if _, ok := available[ruleName]; ok {
				continue
			}
			issues = append(issues, domain.DatasetProfileValidationIssue{
				Severity:    "error",
				Code:        fieldName + "_missing",
				Message:     fmt.Sprintf("%s 의 %s %q 가 worker rule catalog에 없습니다.", owner, fieldName, ruleName),
				Scope:       scope,
				ResourceRef: resourceRef,
			})
		}
	}
	if s.profileRegistry == nil {
		issues = append(issues, domain.DatasetProfileValidationIssue{
			Severity: "warning",
			Code:     "registry_missing",
			Message:  "dataset profile registry가 비어 있어 version 생성 시 명시적 profile만 사용됩니다.",
		})
	} else {
		for dataType, profileID := range s.profileRegistry.Defaults {
			if strings.TrimSpace(profileID) == "" {
				issues = append(issues, domain.DatasetProfileValidationIssue{
					Severity:    "error",
					Code:        "default_profile_empty",
					Message:     fmt.Sprintf("defaults.%s 가 비어 있습니다.", dataType),
					Scope:       "registry_default",
					ResourceRef: "defaults." + dataType,
				})
				continue
			}
			if s.profileRegistry.profileByID(profileID) == nil {
				issues = append(issues, domain.DatasetProfileValidationIssue{
					Severity:    "error",
					Code:        "default_profile_unknown",
					Message:     fmt.Sprintf("defaults.%s 가 존재하지 않는 profile %q 를 가리킵니다.", dataType, profileID),
					Scope:       "registry_default",
					ResourceRef: "defaults." + dataType,
				})
			}
		}
		for profileKey, profile := range s.profileRegistry.Profiles {
			resourceRef := "profiles/" + strings.TrimSpace(profileKey)
			effectiveID := strings.TrimSpace(profile.ProfileID)
			if effectiveID == "" {
				issues = append(issues, domain.DatasetProfileValidationIssue{
					Severity:    "warning",
					Code:        "profile_id_inferred",
					Message:     fmt.Sprintf("profile %q 는 profile_id 가 비어 있어 key 값으로 해석됩니다.", profileKey),
					Scope:       "profile",
					ResourceRef: resourceRef,
				})
				effectiveID = strings.TrimSpace(profileKey)
			}
			if effectiveID != strings.TrimSpace(profileKey) {
				issues = append(issues, domain.DatasetProfileValidationIssue{
					Severity:    "warning",
					Code:        "profile_id_mismatch",
					Message:     fmt.Sprintf("profile key %q 와 profile_id %q 가 다릅니다.", profileKey, effectiveID),
					Scope:       "profile",
					ResourceRef: resourceRef,
				})
			}
			validatePromptVersion(fmt.Sprintf("profile %q", profileKey), "profile", resourceRef, "prepare_prompt", profile.PreparePromptVersion, "prepare", "prepare_batch")
			validatePromptVersion(fmt.Sprintf("profile %q", profileKey), "profile", resourceRef, "sentiment_prompt", profile.SentimentPromptVersion, "sentiment", "sentiment_batch")
			validateRuleNames(fmt.Sprintf("profile %q", profileKey), "profile", resourceRef, "regex_rule", profile.RegexRuleNames, availablePrepareRules)
			validateRuleNames(fmt.Sprintf("profile %q", profileKey), "profile", resourceRef, "garbage_rule", profile.GarbageRuleNames, availableGarbageRules)
		}
	}
	if scanIssues, err := s.validateExistingDatasetVersions(promptMetadata, availablePrepareRules, availableGarbageRules); err != nil {
		return domain.DatasetProfileValidationResponse{}, err
	} else {
		issues = append(issues, scanIssues...)
	}

	for _, issue := range issues {
		if issue.Severity == "error" {
			response.Valid = false
			break
		}
	}
	response.Issues = issues
	return response, nil
}

func (s *DatasetService) GetDatasetProfileRegistry() (domain.DatasetProfileRegistryView, error) {
	validation, err := s.ValidateDatasetProfiles()
	if err != nil {
		return domain.DatasetProfileRegistryView{}, err
	}
	return validation.Registry, nil
}

func (s *DatasetService) validateExistingDatasetVersions(
	promptMetadata map[string]domain.PromptTemplateMetadata,
	availablePrepareRules map[string]struct{},
	availableGarbageRules map[string]struct{},
) ([]domain.DatasetProfileValidationIssue, error) {
	projects, err := s.store.ListProjects()
	if err != nil {
		return nil, err
	}
	issues := make([]domain.DatasetProfileValidationIssue, 0)
	validateRuleNames := func(owner, resourceRef, fieldName string, values []string, available map[string]struct{}) {
		if len(values) == 0 || len(available) == 0 {
			return
		}
		for _, value := range values {
			ruleName := strings.TrimSpace(value)
			if ruleName == "" {
				continue
			}
			if _, ok := available[ruleName]; ok {
				continue
			}
			issues = append(issues, domain.DatasetProfileValidationIssue{
				Severity:    "error",
				Code:        fieldName + "_missing",
				Message:     fmt.Sprintf("%s 의 %s %q 가 현재 worker rule catalog에 없습니다.", owner, fieldName, ruleName),
				Scope:       "dataset_version",
				ResourceRef: resourceRef,
			})
		}
	}
	for _, project := range projects {
		validatePromptVersion := func(owner, resourceRef, fieldName string, value *string, allowedOperations ...string) {
			trimmed := strings.TrimSpace(optionalStringValue(value))
			if trimmed == "" {
				return
			}
			requiredOperation := ""
			if len(allowedOperations) > 0 {
				requiredOperation = strings.TrimSpace(allowedOperations[0])
			}
			if s.projectHasPromptVersion(project.ProjectID, trimmed, allowedOperations...) {
				if requiredOperation == "" || s.projectHasPromptVersion(project.ProjectID, trimmed, requiredOperation) {
					return
				}
				issues = append(issues, domain.DatasetProfileValidationIssue{
					Severity:    "error",
					Code:        fieldName + "_missing",
					Message:     fmt.Sprintf("%s 의 %s %q 는 프로젝트 prompt registry에 %s 템플릿이 없습니다.", owner, fieldName, trimmed, requiredOperation),
					Scope:       "dataset_version",
					ResourceRef: resourceRef,
				})
				return
			}
			meta, ok := promptMetadata[trimmed]
			if !ok {
				issues = append(issues, domain.DatasetProfileValidationIssue{
					Severity:    "error",
					Code:        fieldName + "_missing",
					Message:     fmt.Sprintf("%s 의 %s %q 가 현재 prompt registry에 없습니다.", owner, fieldName, trimmed),
					Scope:       "dataset_version",
					ResourceRef: resourceRef,
				})
				return
			}
			for _, allowed := range allowedOperations {
				if strings.TrimSpace(meta.Operation) == allowed {
					return
				}
			}
			issues = append(issues, domain.DatasetProfileValidationIssue{
				Severity:    "error",
				Code:        fieldName + "_operation_mismatch",
				Message:     fmt.Sprintf("%s 의 %s %q 는 %s 작업용 prompt가 아닙니다.", owner, fieldName, trimmed, strings.Join(allowedOperations, "/")),
				Scope:       "dataset_version",
				ResourceRef: resourceRef,
			})
		}
		datasets, err := s.store.ListDatasets(project.ProjectID)
		if err != nil {
			return nil, err
		}
		for _, dataset := range datasets {
			versions, err := s.store.ListDatasetVersions(project.ProjectID, dataset.DatasetID)
			if err != nil {
				return nil, err
			}
			for _, version := range versions {
				resourceRef := fmt.Sprintf("projects/%s/datasets/%s/versions/%s", project.ProjectID, dataset.DatasetID, version.DatasetVersionID)
				owner := fmt.Sprintf("dataset version %q", version.DatasetVersionID)
				if version.Profile != nil {
					validatePromptVersion(owner, resourceRef, "prepare_prompt", version.Profile.PreparePromptVersion, "prepare", "prepare_batch")
					validatePromptVersion(owner, resourceRef, "sentiment_prompt", version.Profile.SentimentPromptVersion, "sentiment", "sentiment_batch")
					validateRuleNames(owner, resourceRef, "regex_rule", version.Profile.RegexRuleNames, availablePrepareRules)
					validateRuleNames(owner, resourceRef, "garbage_rule", version.Profile.GarbageRuleNames, availableGarbageRules)
					profileID := strings.TrimSpace(version.Profile.ProfileID)
					if profileID != "" && s.profileRegistry != nil && s.profileRegistry.profileByID(profileID) == nil {
						issues = append(issues, domain.DatasetProfileValidationIssue{
							Severity:    "warning",
							Code:        "dataset_version_profile_unknown",
							Message:     fmt.Sprintf("%s 가 현재 registry에 없는 profile_id %q 를 참조합니다.", owner, profileID),
							Scope:       "dataset_version",
							ResourceRef: resourceRef,
						})
					}
				}
				validatePromptVersion(owner, resourceRef, "prepare_prompt", version.PreparePromptVer, "prepare", "prepare_batch")
				validatePromptVersion(owner, resourceRef, "sentiment_prompt", version.SentimentPromptVer, "sentiment", "sentiment_batch")
			}
		}
	}
	return issues, nil
}

func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		result[trimmed] = struct{}{}
	}
	return result
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]string, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneProfileMap(input map[string]domain.DatasetProfile) map[string]domain.DatasetProfile {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]domain.DatasetProfile, len(input))
	for key, value := range input {
		profile := value
		if normalized := normalizeDatasetProfile(&profile); normalized != nil {
			output[key] = *normalized
			continue
		}
		output[key] = value
	}
	return output
}

func slicesSortStrings(values []string) {
	if len(values) < 2 {
		return
	}
	sort.Strings(values)
}
