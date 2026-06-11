package service

import (
	"encoding/json"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/store"
)

// 보고서 문서 CRUD (silverone 2026-06-11).
//
// saved_results(분석 결과 보관함)를 조합해 만든 별도 문서. blocks는 작성 당시
// snapshot을 복제해 담는 opaque JSON 배열 — control-plane은 영속만 책임지고
// 블록 contract는 프론트(보고서 에디터)가 소유한다. 1차는 CRUD만; 공유/재생성/
// export는 후속. project 스코프(보고서 탭이 project 단위).

const reportTitleMaxLen = 200

// validateReportBlocks — blocks가 오면 JSON 배열인지만 검증(구조 강제는 안 함).
// 비어 있으면 빈 배열로 정규화. 배열이 아니면 ErrInvalidArgument.
func validateReportBlocks(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage("[]"), nil
	}
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return nil, ErrInvalidArgument{Message: "blocks must be a JSON array"}
	}
	return raw, nil
}

func reportTitleOrDefault(title string) string {
	t := strings.TrimSpace(title)
	if t == "" {
		t = "제목 없는 보고서"
	}
	return truncateRunes(t, reportTitleMaxLen)
}

func (s *DatasetService) requireProject(projectID string) error {
	if _, err := s.store.GetProject(projectID); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "project"}
		}
		return err
	}
	return nil
}

func (s *DatasetService) CreateReport(projectID string, input domain.ReportCreateRequest) (domain.Report, error) {
	if err := s.requireProject(projectID); err != nil {
		return domain.Report{}, err
	}
	blocks, err := validateReportBlocks(input.Blocks)
	if err != nil {
		return domain.Report{}, err
	}
	now := time.Now().UTC()
	report := domain.Report{
		ReportID:  id.New(),
		ProjectID: projectID,
		Title:     reportTitleOrDefault(input.Title),
		Blocks:    blocks,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := s.store.CreateReport(report); err != nil {
		return domain.Report{}, err
	}
	return s.store.GetReport(projectID, report.ReportID)
}

func (s *DatasetService) ListReports(projectID string) (domain.ReportListResponse, error) {
	if err := s.requireProject(projectID); err != nil {
		return domain.ReportListResponse{}, err
	}
	items, err := s.store.ListReports(projectID)
	if err != nil {
		return domain.ReportListResponse{}, err
	}
	return domain.ReportListResponse{Items: items}, nil
}

func (s *DatasetService) GetReport(projectID, reportID string) (domain.Report, error) {
	report, err := s.store.GetReport(projectID, strings.TrimSpace(reportID))
	if err != nil {
		if err == store.ErrNotFound {
			return domain.Report{}, ErrNotFound{Resource: "report"}
		}
		return domain.Report{}, err
	}
	return report, nil
}

func (s *DatasetService) UpdateReport(projectID, reportID string, input domain.ReportUpdateRequest) (domain.Report, error) {
	reportID = strings.TrimSpace(reportID)
	blocks, err := validateReportBlocks(input.Blocks)
	if err != nil {
		return domain.Report{}, err
	}
	if err := s.store.UpdateReport(domain.Report{
		ReportID:  reportID,
		ProjectID: projectID,
		Title:     reportTitleOrDefault(input.Title),
		Blocks:    blocks,
		UpdatedAt: time.Now().UTC(),
	}); err != nil {
		if err == store.ErrNotFound {
			return domain.Report{}, ErrNotFound{Resource: "report"}
		}
		return domain.Report{}, err
	}
	return s.store.GetReport(projectID, reportID)
}

func (s *DatasetService) DeleteReport(projectID, reportID string) error {
	if err := s.store.DeleteReport(projectID, strings.TrimSpace(reportID)); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "report"}
		}
		return err
	}
	return nil
}
