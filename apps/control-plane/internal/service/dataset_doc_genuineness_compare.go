package service

import (
	"os"
	"sort"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

// doc_genuineness 모델 비교 (silverone 2026-06-15). 한 버전에 보관된 두 모델
// 결과(run)를 doc_id 기준 1:1로 비교한다. 비교값은 artifact 원본(=모델 라벨)이라
// override(사람 보정)에 오염되지 않는다. override는 doc_id별 정답 힌트로만 붙인다.
// 같은 버전 = 같은 원본이라 doc_id가 정확히 정렬된다.

// compareTiers — confusion matrix 행/열 순서이자 비교 대상 tier 집합.
var compareTiers = []string{"genuine_review", "mixed", "non_review", "uncertain"}

// compareHighAgreement — 정답이 없을 때 "일치율 높음(agreement_only)" vs "검토
// 필요(review_needed)"를 가르는 임계. 그 이하면 모델 간 기준 차가 크다고 본다.
const compareHighAgreement = 0.85

// compareDocRow — 비교용 doc 1건(원본 모델 라벨).
type compareDocRow struct {
	label       string
	reason      string
	cleanedText string
}

// loadRunLabels — run artifact(ref)의 전체 doc 라벨을 doc_id 맵으로 로드한다.
func (s *DatasetService) loadRunLabels(ref, cleanRef, versionID string) (map[string]compareDocRow, error) {
	_, _, _, items, err := loadDocGenuinenessArtifact(ref, cleanRef, 1<<30, 0, versionID, "")
	if err != nil {
		return nil, err
	}
	rows := make(map[string]compareDocRow, len(items))
	for _, item := range items {
		docID, _ := item["doc_id"].(string)
		if docID == "" {
			continue
		}
		label, _ := item["genuineness"].(string)
		reason, _ := item["reason"].(string)
		cleaned, _ := item["cleaned_text"].(string)
		rows[docID] = compareDocRow{label: label, reason: reason, cleanedText: cleaned}
	}
	return rows, nil
}

// CompareDocGenuineness — 한 버전 안의 두 모델 결과를 비교. limit/offset은
// 불일치 목록에만 적용된다.
func (s *DatasetService) CompareDocGenuineness(
	projectID, datasetID, versionID, modelA, modelB string,
	limit, offset int,
) (domain.DocGenuinenessCompareView, error) {
	modelA = strings.TrimSpace(modelA)
	modelB = strings.TrimSpace(modelB)
	if versionID == "" || modelA == "" || modelB == "" {
		return domain.DocGenuinenessCompareView{}, ErrInvalidArgument{Message: "version_id, model_a, model_b are required"}
	}
	if modelA == modelB {
		return domain.DocGenuinenessCompareView{}, ErrInvalidArgument{Message: "model_a and model_b must differ"}
	}
	limit, offset = normalizeArtifactPagination(limit, offset)

	version, err := s.GetDatasetVersion(projectID, datasetID, versionID)
	if err != nil {
		return domain.DocGenuinenessCompareView{}, err
	}
	runs := docGenuinenessRunsFromMetadata(version.Metadata)
	runA, okA := findDocGenuinenessRun(runs, modelA)
	if !okA {
		return domain.DocGenuinenessCompareView{}, ErrInvalidArgument{Message: "no doc_genuineness result for model_a: " + modelA}
	}
	runB, okB := findDocGenuinenessRun(runs, modelB)
	if !okB {
		return domain.DocGenuinenessCompareView{}, ErrInvalidArgument{Message: "no doc_genuineness result for model_b: " + modelB}
	}

	cleanRef := strings.TrimSpace(metadataString(version.Metadata, "clean_uri", ""))
	if cleanRef == "" {
		cleanRef = strings.TrimSpace(metadataString(version.Metadata, "cleaned_ref", ""))
	}
	if cleanRef != "" {
		if _, statErr := os.Stat(cleanRef); statErr != nil {
			cleanRef = "" // 본문 join 생략
		}
	}

	rowsA, err := s.loadRunLabels(runA.Ref, cleanRef, versionID)
	if err != nil {
		return domain.DocGenuinenessCompareView{}, err
	}
	rowsB, err := s.loadRunLabels(runB.Ref, cleanRef, versionID)
	if err != nil {
		return domain.DocGenuinenessCompareView{}, err
	}

	// override(정답 힌트) — 버전 스코프, 모델 무관. 비교값은 오염시키지 않고 표시만.
	overrideByDoc := map[string]string{}
	if ovs, ovErr := s.store.ListDocGenuinenessOverrides(projectID, versionID); ovErr == nil {
		for _, o := range ovs {
			overrideByDoc[o.DocID] = o.OverrideGenuineness
		}
	} else {
		return domain.DocGenuinenessCompareView{}, ovErr
	}

	tierIdx := make(map[string]int, len(compareTiers))
	for i, t := range compareTiers {
		tierIdx[t] = i
	}
	confusion := make([][]int, len(compareTiers))
	for i := range confusion {
		confusion[i] = make([]int, len(compareTiers))
	}

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

	var disagreements []domain.DocGenuinenessCompareDisagreement
	compared, matched, onlyInA, onlyInB := 0, 0, 0, 0
	// override(정답) 기준 모델별 정확도 누적 + 우선 검토(정답 미보정 불일치) 카운트.
	ovSample, ovACorrect, ovBCorrect, unreviewed := 0, 0, 0, 0
	for _, docID := range docIDs {
		a, okA := rowsA[docID]
		b, okB := rowsB[docID]
		switch {
		case okA && !okB:
			onlyInA++
		case !okA && okB:
			onlyInB++
		default:
			compared++
			if ai, ok := tierIdx[a.label]; ok {
				if bi, ok := tierIdx[b.label]; ok {
					confusion[ai][bi]++
				}
			}
			// 정답이 있는 문서는 모델별 정확도에 반영(일치/불일치 무관).
			if truth, ok := overrideByDoc[docID]; ok && truth != "" {
				ovSample++
				if a.label == truth {
					ovACorrect++
				}
				if b.label == truth {
					ovBCorrect++
				}
			}
			if a.label == b.label {
				matched++
				continue
			}
			if overrideByDoc[docID] == "" {
				unreviewed++ // 정답 미보정 불일치 → 사람이 봐야 함
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
				OverrideGenuineness: overrideByDoc[docID],
			})
		}
	}

	rate := 0.0
	if compared > 0 {
		rate = float64(matched) / float64(compared)
	}

	// 불일치 패턴(off-diagonal) 빈도 내림차순.
	patterns := make([]domain.DocGenuinenessComparePattern, 0)
	for i, ai := range compareTiers {
		for j, bj := range compareTiers {
			if i != j && confusion[i][j] > 0 {
				patterns = append(patterns, domain.DocGenuinenessComparePattern{
					AGenuineness: ai, BGenuineness: bj, Count: confusion[i][j],
				})
			}
		}
	}
	sort.SliceStable(patterns, func(i, j int) bool { return patterns[i].Count > patterns[j].Count })

	// override 정확도 + 판정 레벨.
	var overrideEval *domain.DocGenuinenessOverrideEval
	if ovSample > 0 {
		aAcc := float64(ovACorrect) / float64(ovSample)
		bAcc := float64(ovBCorrect) / float64(ovSample)
		leader := "tie"
		if ovACorrect > ovBCorrect {
			leader = "a"
		} else if ovBCorrect > ovACorrect {
			leader = "b"
		}
		overrideEval = &domain.DocGenuinenessOverrideEval{
			SampleCount: ovSample, ACorrect: ovACorrect, BCorrect: ovBCorrect,
			AAccuracy: aAcc, BAccuracy: bAcc, Leader: leader,
		}
	}
	verdictLevel := "review_needed"
	switch {
	case overrideEval != nil:
		verdictLevel = "ground_truth"
	case rate >= compareHighAgreement:
		verdictLevel = "agreement_only"
	}

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
			DatasetVersionID: versionID,
			Model:            modelA,
			ModelDisplayName: s.modelDisplayNameFor(modelA),
			Total:            len(rowsA),
		},
		VersionB: domain.DocGenuinenessCompareSide{
			DatasetVersionID: versionID,
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
		Confusion:               confusion,
		Disagreements:           page,
		DisagreementsTotal:      disTotal,
		Pagination:              &domain.ArtifactPagination{Limit: limit, Offset: offset, Total: disTotal},
		Patterns:                patterns,
		OverrideEval:            overrideEval,
		UnreviewedDisagreements: unreviewed,
		VerdictLevel:            verdictLevel,
	}, nil
}
