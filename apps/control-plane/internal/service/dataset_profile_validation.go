package service

import (
	"analysis-support-platform/control-plane/internal/domain"
)

// dataset profile registry 관련 utility. ValidateDatasetProfiles / catalog
// endpoint 모두 제거된 후에도 dataset_versions / analysis 흐름에서 활성
// profile lookup + metadata clone에 필요한 helper만 남긴다.

func (s *DatasetService) resolveDatasetProfile(dataType string, explicit *domain.DatasetProfile) *domain.DatasetProfile {
	return s.profileRegistry.resolve(dataType, explicit)
}
