package service

import (
	"sort"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

// doc_genuineness 모델 비교 (silverone 2026-06-15). 같은 원본을 두 모델로 빌드한
// 두 버전의 진성 분류를 doc_id 기준 1:1로 비교한다. 비교값은 override 적용 전
// *원본 모델 라벨* — override(사람 보정)는 모델 간 비교를 오염시키므로 제외하고
// 정답 힌트(override_genuineness)로만 노출한다. doc_id는 빌드 간 안정적이라
// (={version_id}:row:N) 모델이 달라도 1:1 정렬된다.

// compareTiers — confusion matrix 행/열 순서이자 비교 대상 tier 집합.
var compareTiers = []string{"genuine_review", "mixed", "non_review", "uncertain"}

// docGenuinenessArtifactAvailable — resolveArtifactStatus 어휘 기준 artifact가
// 조회 가능한 상태인지. "completed"(job row 유무 무관 artifact 존재) / "ready"
// (옛 metadata 값) 둘 다 허용. queued/running/failed/not_started는 비교 불가.
func docGenuinenessArtifactAvailable(status string) bool {
	return status == "completed" || status == "ready"
}

// compareDocRow — 비교용 doc 1건. label은 override 적용 전 원본 모델 라벨.
type compareDocRow struct {
	label         string
	reason        string
	cleanedText   string
	overrideLabel string // 사람 보정이 있으면 그 값(정답 힌트), 없으면 ""
}

// collectDocGenuinenessForCompare — 한 버전의 모든 doc 원본 라벨을 회수한다.
// GetDocGenuinenessView를 페이지 단위로 호출해 cleaned_text/override까지 일관되게
// 얻고, override가 적용된 item에서는 original_* 필드로 원본 모델 라벨을 복원한다.
func (s *DatasetService) collectDocGenuinenessForCompare(
	projectID, datasetID, versionID string,
) (model string, status string, rows map[string]compareDocRow, order []string, err error) {
	const page = 1000
	rows = map[string]compareDocRow{}
	offset := 0
	for {
		view, e := s.GetDocGenuinenessView(projectID, datasetID, versionID, page, offset, "")
		if e != nil {
			return "", "", nil, nil, e
		}
		status = view.Status
		if view.Applied != nil {
			if m, ok := view.Applied["model"].(string); ok {
				model = m
			}
		}
		for _, item := range view.Items {
			docID, _ := item["doc_id"].(string)
			if docID == "" {
				continue
			}
			label, _ := item["genuineness"].(string)
			reason, _ := item["reason"].(string)
			overrideLabel := ""
			if ov, _ := item["is_overridden"].(bool); ov {
				// override 적용된 item — 원본 모델 라벨/사유로 되돌린다.
				if orig, ok := item["original_genuineness"].(string); ok && orig != "" {
					label = orig
				}
				if origReason, ok := item["original_reason"].(string); ok {
					reason = origReason
				}
				overrideLabel, _ = item["override_genuineness"].(string)
			}
			cleaned, _ := item["cleaned_text"].(string)
			if _, seen := rows[docID]; !seen {
				order = append(order, docID)
			}
			rows[docID] = compareDocRow{
				label:         label,
				reason:        reason,
				cleanedText:   cleaned,
				overrideLabel: overrideLabel,
			}
		}
		total := 0
		if view.Pagination != nil {
			total = view.Pagination.Total
		}
		offset += page
		if len(view.Items) == 0 || offset >= total {
			break
		}
	}
	return model, status, rows, order, nil
}

// CompareDocGenuineness — 두 버전의 진성 분류를 비교한 리포트. limit/offset은
// 불일치 목록에만 적용된다.
func (s *DatasetService) CompareDocGenuineness(
	projectID, datasetID, versionAID, versionBID string,
	limit, offset int,
) (domain.DocGenuinenessCompareView, error) {
	if strings.TrimSpace(versionAID) == "" || strings.TrimSpace(versionBID) == "" {
		return domain.DocGenuinenessCompareView{}, ErrInvalidArgument{Message: "version_a and version_b are required"}
	}
	if versionAID == versionBID {
		return domain.DocGenuinenessCompareView{}, ErrInvalidArgument{Message: "version_a and version_b must differ"}
	}
	limit, offset = normalizeArtifactPagination(limit, offset)

	modelA, statusA, rowsA, _, err := s.collectDocGenuinenessForCompare(projectID, datasetID, versionAID)
	if err != nil {
		return domain.DocGenuinenessCompareView{}, err
	}
	if !docGenuinenessArtifactAvailable(statusA) {
		return domain.DocGenuinenessCompareView{}, ErrInvalidArgument{Message: "version_a doc_genuineness not ready (status: " + statusA + ")"}
	}
	modelB, statusB, rowsB, _, err := s.collectDocGenuinenessForCompare(projectID, datasetID, versionBID)
	if err != nil {
		return domain.DocGenuinenessCompareView{}, err
	}
	if !docGenuinenessArtifactAvailable(statusB) {
		return domain.DocGenuinenessCompareView{}, ErrInvalidArgument{Message: "version_b doc_genuineness not ready (status: " + statusB + ")"}
	}

	tierIdx := make(map[string]int, len(compareTiers))
	for i, t := range compareTiers {
		tierIdx[t] = i
	}
	confusion := make([][]int, len(compareTiers))
	for i := range confusion {
		confusion[i] = make([]int, len(compareTiers))
	}

	var disagreements []domain.DocGenuinenessCompareDisagreement
	compared, matched, onlyInA, onlyInB := 0, 0, 0, 0

	// 정렬된 doc_id 순회로 결과 결정론적.
	docIDs := make([]string, 0, len(rowsA)+len(rowsB))
	seen := map[string]struct{}{}
	for id := range rowsA {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			docIDs = append(docIDs, id)
		}
	}
	for id := range rowsB {
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			docIDs = append(docIDs, id)
		}
	}
	sort.Strings(docIDs)

	for _, docID := range docIDs {
		a, okA := rowsA[docID]
		b, okB := rowsB[docID]
		switch {
		case okA && !okB:
			onlyInA++
		case !okA && okB:
			onlyInB++
		default: // 양쪽 모두 존재
			compared++
			if ai, ok := tierIdx[a.label]; ok {
				if bi, ok := tierIdx[b.label]; ok {
					confusion[ai][bi]++
				}
			}
			if a.label == b.label {
				matched++
				continue
			}
			// override는 어느 쪽 버전에 찍혔든 같은 doc의 정답이므로 둘 중 하나 채택.
			override := a.overrideLabel
			if override == "" {
				override = b.overrideLabel
			}
			cleaned := a.cleanedText
			if cleaned == "" {
				cleaned = b.cleanedText
			}
			disagreements = append(disagreements, domain.DocGenuinenessCompareDisagreement{
				DocID:               docID,
				AGenuineness:        a.label,
				AReason:             a.reason,
				BGenuineness:        b.label,
				BReason:             b.reason,
				CleanedText:         cleaned,
				OverrideGenuineness: override,
			})
		}
	}

	rate := 0.0
	if compared > 0 {
		rate = float64(matched) / float64(compared)
	}

	// 불일치 목록 페이징.
	disTotal := len(disagreements)
	start := offset
	if start > disTotal {
		start = disTotal
	}
	end := start + limit
	if end > disTotal {
		end = disTotal
	}
	page := disagreements[start:end]
	if page == nil {
		page = []domain.DocGenuinenessCompareDisagreement{}
	}

	return domain.DocGenuinenessCompareView{
		VersionA: domain.DocGenuinenessCompareSide{
			DatasetVersionID: versionAID,
			Model:            modelA,
			ModelDisplayName: s.modelDisplayNameFor(modelA),
			Total:            len(rowsA),
		},
		VersionB: domain.DocGenuinenessCompareSide{
			DatasetVersionID: versionBID,
			Model:            modelB,
			ModelDisplayName: s.modelDisplayNameFor(modelB),
			Total:            len(rowsB),
		},
		Tiers:              append([]string(nil), compareTiers...),
		Compared:           compared,
		Matched:            matched,
		Rate:               rate,
		OnlyInA:            onlyInA,
		OnlyInB:            onlyInB,
		Confusion:          confusion,
		Disagreements:      page,
		DisagreementsTotal: disTotal,
		Pagination:         &domain.ArtifactPagination{Limit: limit, Offset: offset, Total: disTotal},
	}, nil
}
