package service

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/store"
)

// 진성 라벨 수동 보정 overlay (silverone 2026-06-11).
//
// artifact JSONL(LLOA 원본)은 건드리지 않고, 운영자 보정값만 doc_genuineness_
// overrides에 저장한다. 진성 분석 GET이 effective label로 합성하고, summary도
// effective 기준으로 재집계한다. 보정 시점의 artifact 라벨을 original로 snapshot
// 해 두어 summary 재집계·감사·재현이 가능하다. version 스코프.

// 보정 허용 tier. genuine_review / non_review / uncertain.
// silverone 2026-06-16 — legacy mixed tier 제거.
var allowedGenuinenessTiers = map[string]bool{
	"genuine_review": true,
	"non_review":     true,
	"uncertain":      true,
}

// 수정 사유 미입력 시 서버 기본값.
const defaultOverrideReason = "운영자 수동 수정"

// clauseLabelIncludedTier — clause_label build 기본 포함 집합(genuine_review +
// uncertain). 이 경계를 넘는 보정은 절/감성/키워드 분석 재실행 권장 대상이다.
func clauseLabelIncludedTier(tier string) bool {
	return tier == "genuine_review" || tier == "uncertain"
}

func docGenuinenessRefFromMetadata(metadata map[string]any) string {
	ref := strings.TrimSpace(metadataString(metadata, "doc_genuineness_ref", ""))
	if ref == "" {
		ref = strings.TrimSpace(metadataString(metadata, "doc_genuineness_uri", ""))
	}
	return ref
}

// docGenuinenessOriginalForDoc — artifact에서 해당 doc의 원본 라벨+사유 1건 조회.
// found=false면 artifact에 그 doc_id가 없음(404 처리).
func docGenuinenessOriginalForDoc(ref, docID string) (genuineness, reason string, found bool, err error) {
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		return "", "", false, err
	}
	defer cleanup()
	source := fmt.Sprintf("read_json('%s', format='newline_delimited')", escapeDuckDBLiteral(ref))
	query := fmt.Sprintf(
		"SELECT genuineness, reason FROM %s WHERE doc_id = '%s' LIMIT 1",
		source, escapeDuckDBLiteral(docID),
	)
	var g, r sql.NullString
	if err := db.QueryRow(query).Scan(&g, &r); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	return strings.TrimSpace(g.String), strings.TrimSpace(r.String), true, nil
}

// SetDocGenuinenessOverride — doc의 진성 라벨을 보정한다. original 라벨/사유는
// artifact에서 서버가 직접 조회해 snapshot한다(client trust 배제). 수정 사유가
// 비면 "운영자 수동 수정" 기본값으로 저장한다. 보정 해제는 DELETE override로.
func (s *DatasetService) SetDocGenuinenessOverride(
	projectID, datasetID, datasetVersionID, docID string,
	req domain.DocGenuinenessOverrideRequest,
) (domain.DocGenuinenessOverride, error) {
	docID = strings.TrimSpace(docID)
	if docID == "" {
		return domain.DocGenuinenessOverride{}, ErrInvalidArgument{Message: "doc_id is required"}
	}
	tier := strings.TrimSpace(req.Genuineness)
	if !allowedGenuinenessTiers[tier] {
		return domain.DocGenuinenessOverride{}, ErrInvalidArgument{Message: "genuineness must be one of genuine_review / non_review / uncertain"}
	}
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DocGenuinenessOverride{}, err
	}
	ref := docGenuinenessRefFromMetadata(version.Metadata)
	if ref == "" {
		return domain.DocGenuinenessOverride{}, ErrInvalidArgument{Message: "doc_genuineness artifact not ready — cannot override before genuineness build"}
	}
	if _, statErr := os.Stat(ref); statErr != nil {
		return domain.DocGenuinenessOverride{}, ErrInvalidArgument{Message: "doc_genuineness artifact not available"}
	}
	originalGenuineness, originalReason, found, err := docGenuinenessOriginalForDoc(ref, docID)
	if err != nil {
		return domain.DocGenuinenessOverride{}, err
	}
	if !found {
		return domain.DocGenuinenessOverride{}, ErrNotFound{Resource: "doc_genuineness doc"}
	}

	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = defaultOverrideReason
	}
	now := time.Now().UTC()
	override := domain.DocGenuinenessOverride{
		ProjectID:           projectID,
		DatasetID:           datasetID,
		DatasetVersionID:    datasetVersionID,
		DocID:               docID,
		OriginalGenuineness: originalGenuineness,
		OriginalReason:      originalReason,
		OverrideGenuineness: tier,
		OverrideReason:      reason,
		CreatedAt:           now,
		UpdatedAt:           now,
	}
	if err := s.store.UpsertDocGenuinenessOverride(override); err != nil {
		return domain.DocGenuinenessOverride{}, err
	}
	return override, nil
}

// DeleteDocGenuinenessOverride — 보정 되돌리기(원본 라벨로 복귀).
func (s *DatasetService) DeleteDocGenuinenessOverride(projectID, datasetID, datasetVersionID, docID string) error {
	// version 존재 검증(404 정합성).
	if _, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID); err != nil {
		return err
	}
	if err := s.store.DeleteDocGenuinenessOverride(projectID, datasetVersionID, strings.TrimSpace(docID)); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "doc_genuineness override"}
		}
		return err
	}
	return nil
}

// applyDocGenuinenessOverrides — 진성 view에 effective label을 합성한다.
//   - 현재 페이지 items: 보정된 doc의 genuineness/reason을 effective(override)로
//     교체하고 original_genuineness/original_reason/override_genuineness/
//     override_reason/is_overridden을 부가. 화면·필터는 effective genuineness/
//     reason 필드를 그대로 쓴다.
//   - summary.genuineness: 전체 보정 기준으로 tier 카운트 재집계(total 불변)
//   - summary.override_count / downstream_boundary_crossed 부가
//
// 페이지·필터는 원본 라벨 기준 유지(MVP) — 정확 effective 필터/페이지네이션은 후속.
func applyDocGenuinenessOverrides(view *domain.DatasetArtifactView, overrides []domain.DocGenuinenessOverride) bool {
	if len(overrides) == 0 {
		return false
	}
	byDoc := make(map[string]domain.DocGenuinenessOverride, len(overrides))
	for _, o := range overrides {
		byDoc[o.DocID] = o
	}
	for _, item := range view.Items {
		docID, _ := item["doc_id"].(string)
		o, ok := byDoc[docID]
		if !ok {
			continue
		}
		// 원본은 artifact item 값 우선, 없으면 보정 시 snapshot 값으로.
		originalGenuineness, _ := item["genuineness"].(string)
		if originalGenuineness == "" {
			originalGenuineness = o.OriginalGenuineness
		}
		originalReason, _ := item["reason"].(string)
		if originalReason == "" {
			originalReason = o.OriginalReason
		}
		item["original_genuineness"] = originalGenuineness
		item["original_reason"] = originalReason
		item["override_genuineness"] = o.OverrideGenuineness
		item["override_reason"] = o.OverrideReason
		item["is_overridden"] = true
		// effective — 화면/필터가 바로 쓰는 기본 필드.
		item["genuineness"] = o.OverrideGenuineness
		item["reason"] = o.OverrideReason
	}

	crossed := false
	if view.Summary != nil {
		if counts, ok := view.Summary["genuineness"].(map[string]int); ok {
			for _, o := range overrides {
				if o.OriginalGenuineness != "" {
					counts[o.OriginalGenuineness]--
					if counts[o.OriginalGenuineness] < 0 {
						counts[o.OriginalGenuineness] = 0
					}
				}
				counts[o.OverrideGenuineness]++
				if clauseLabelIncludedTier(o.OriginalGenuineness) != clauseLabelIncludedTier(o.OverrideGenuineness) {
					crossed = true
				}
			}
			view.Summary["genuineness"] = counts
		}
		view.Summary["override_count"] = len(overrides)
		view.Summary["downstream_boundary_crossed"] = crossed
	}
	return crossed
}
