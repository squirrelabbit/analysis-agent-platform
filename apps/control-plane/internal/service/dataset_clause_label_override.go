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

// 절 라벨링 aspect/sentiment 수동 보정 overlay (silverone 2026-06-11).
//
// artifact JSONL(LLOA 원본)은 건드리지 않고, 운영자 보정값만 clause_label_
// overrides에 저장한다. 절 라벨링 GET이 effective aspect/sentiment로 합성하고,
// summary(sentiment/aspect/aspect_sentiment)도 effective 기준으로 재집계한다.
// clause_id는 뷰가 doc_id + 절 index(ROW_NUMBER)로 합성하는 값이라, 보정 시
// 같은 규칙으로 artifact에서 원본 aspect/sentiment를 조회해 snapshot한다.

// 수정 사유 미입력 시 서버 기본값.
const clauseDefaultOverrideReason = "운영자 수동 수정"

func clauseLabelSentimentAllowed(s string) bool {
	for _, v := range clauseLabelStandardSentiments {
		if v == s {
			return true
		}
	}
	return false
}

// clauseLabelOriginalForClause — artifact에서 clause_id의 원본 aspect/sentiment
// 1건 조회. clause_id는 loadClauseLabelArtifact와 동일하게 doc_id + 절 index로
// 합성한다. found=false면 그 clause_id가 없음(404).
func clauseLabelOriginalForClause(ref, clauseID string) (aspect, sentiment string, found bool, err error) {
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		return "", "", false, err
	}
	defer cleanup()
	source := fmt.Sprintf("read_json('%s', format='newline_delimited')", escapeDuckDBLiteral(ref))
	query := fmt.Sprintf(
		`WITH ordered AS (
		    SELECT *, ROW_NUMBER() OVER () AS _rn FROM %s
		 ),
		 numbered AS (
		    SELECT
		       doc_id || '-' || CAST(ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY _rn) - 1 AS VARCHAR) AS clause_id,
		       aspect, sentiment
		    FROM ordered
		 )
		 SELECT aspect, sentiment FROM numbered WHERE clause_id = '%s' LIMIT 1`,
		source, escapeDuckDBLiteral(clauseID),
	)
	var a, s sql.NullString
	if err := db.QueryRow(query).Scan(&a, &s); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", false, nil
		}
		return "", "", false, err
	}
	return strings.TrimSpace(a.String), strings.TrimSpace(s.String), true, nil
}

func clauseLabelRefFromMetadata(metadata map[string]any) string {
	ref := strings.TrimSpace(metadataString(metadata, "clause_label_ref", ""))
	if ref == "" {
		ref = strings.TrimSpace(metadataString(metadata, "clause_label_uri", ""))
	}
	return ref
}

// SetClauseLabelOverride — 절의 aspect/sentiment를 보정한다. aspect/sentiment는
// 둘 다 선택이며 보낸 것만 바뀌고 나머지는 원본 유지(둘 다 비면 거부). original은
// 서버가 artifact에서 직접 조회해 snapshot. 수정 사유 비면 기본값. PATCH는 항상
// upsert(해제는 DELETE override).
func (s *DatasetService) SetClauseLabelOverride(
	projectID, datasetID, datasetVersionID, clauseID string,
	req domain.ClauseLabelOverrideRequest,
) (domain.ClauseLabelOverride, error) {
	clauseID = strings.TrimSpace(clauseID)
	if clauseID == "" {
		return domain.ClauseLabelOverride{}, ErrInvalidArgument{Message: "clause_id is required"}
	}
	aspect := strings.TrimSpace(req.Aspect)
	sentiment := strings.TrimSpace(req.Sentiment)
	if aspect == "" && sentiment == "" {
		return domain.ClauseLabelOverride{}, ErrInvalidArgument{Message: "aspect or sentiment is required"}
	}
	if sentiment != "" && !clauseLabelSentimentAllowed(sentiment) {
		return domain.ClauseLabelOverride{}, ErrInvalidArgument{Message: "sentiment must be one of positive / negative / neutral"}
	}
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.ClauseLabelOverride{}, err
	}
	ref := clauseLabelRefFromMetadata(version.Metadata)
	if ref == "" {
		return domain.ClauseLabelOverride{}, ErrInvalidArgument{Message: "clause_label artifact not ready — cannot override before clause labeling"}
	}
	if _, statErr := os.Stat(ref); statErr != nil {
		return domain.ClauseLabelOverride{}, ErrInvalidArgument{Message: "clause_label artifact not available"}
	}
	origAspect, origSentiment, found, err := clauseLabelOriginalForClause(ref, clauseID)
	if err != nil {
		return domain.ClauseLabelOverride{}, err
	}
	if !found {
		return domain.ClauseLabelOverride{}, ErrNotFound{Resource: "clause_label clause"}
	}

	// 보낸 필드만 effective로, 나머지는 원본 유지.
	overrideAspect := origAspect
	if aspect != "" {
		overrideAspect = aspect
	}
	overrideSentiment := origSentiment
	if sentiment != "" {
		overrideSentiment = sentiment
	}
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = clauseDefaultOverrideReason
	}
	now := time.Now().UTC()
	override := domain.ClauseLabelOverride{
		ProjectID:         projectID,
		DatasetID:         datasetID,
		DatasetVersionID:  datasetVersionID,
		ClauseID:          clauseID,
		OriginalAspect:    origAspect,
		OriginalSentiment: origSentiment,
		OverrideAspect:    overrideAspect,
		OverrideSentiment: overrideSentiment,
		OverrideReason:    reason,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := s.store.UpsertClauseLabelOverride(override); err != nil {
		return domain.ClauseLabelOverride{}, err
	}
	return override, nil
}

// DeleteClauseLabelOverride — 보정 되돌리기(원본 LLOA 라벨로 복귀).
func (s *DatasetService) DeleteClauseLabelOverride(projectID, datasetID, datasetVersionID, clauseID string) error {
	if _, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID); err != nil {
		return err
	}
	if err := s.store.DeleteClauseLabelOverride(projectID, datasetVersionID, strings.TrimSpace(clauseID)); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "clause_label override"}
		}
		return err
	}
	return nil
}

// applyClauseLabelOverrides — 절 라벨링 view에 effective aspect/sentiment를 합성.
//   - 현재 페이지 items: 보정된 clause의 aspect/sentiment를 effective로 교체 +
//     original_*/override_*/is_overridden 부가
//   - summary.sentiment / summary.aspect 플랫 카운트 재집계(total 불변)
//   - summary.aspect_sentiment 교차 분포 재집계(영향 aspect의 total/percent 재계산)
//   - summary.override_count 부가
//
// 페이지·필터는 원본 라벨 기준 유지(MVP) — effective 필터/페이지네이션은 후속.
func applyClauseLabelOverrides(view *domain.DatasetArtifactView, overrides []domain.ClauseLabelOverride) {
	if len(overrides) == 0 {
		return
	}
	byClause := make(map[string]domain.ClauseLabelOverride, len(overrides))
	for _, o := range overrides {
		byClause[o.ClauseID] = o
	}
	for _, item := range view.Items {
		cid, _ := item["clause_id"].(string)
		o, ok := byClause[cid]
		if !ok {
			continue
		}
		origAspect, _ := item["aspect"].(string)
		if origAspect == "" {
			origAspect = o.OriginalAspect
		}
		origSentiment, _ := item["sentiment"].(string)
		if origSentiment == "" {
			origSentiment = o.OriginalSentiment
		}
		item["original_aspect"] = origAspect
		item["original_sentiment"] = origSentiment
		item["override_aspect"] = o.OverrideAspect
		item["override_sentiment"] = o.OverrideSentiment
		item["override_reason"] = o.OverrideReason
		item["is_overridden"] = true
		// effective — 화면/필터가 바로 쓰는 기본 필드.
		item["aspect"] = o.OverrideAspect
		item["sentiment"] = o.OverrideSentiment
	}

	if view.Summary == nil {
		return
	}
	if m, ok := view.Summary["sentiment"].(map[string]int); ok {
		for _, o := range overrides {
			adjustCountMap(m, o.OriginalSentiment, o.OverrideSentiment)
		}
		view.Summary["sentiment"] = m
	}
	if m, ok := view.Summary["aspect"].(map[string]int); ok {
		for _, o := range overrides {
			adjustCountMap(m, o.OriginalAspect, o.OverrideAspect)
		}
		view.Summary["aspect"] = m
	}
	if as, ok := view.Summary["aspect_sentiment"].(map[string]any); ok {
		affected := map[string]bool{}
		for _, o := range overrides {
			adjustAspectSentimentCount(as, o.OriginalAspect, o.OriginalSentiment, -1)
			adjustAspectSentimentCount(as, o.OverrideAspect, o.OverrideSentiment, +1)
			affected[o.OriginalAspect] = true
			affected[o.OverrideAspect] = true
		}
		for aspect := range affected {
			recomputeAspectSentimentPercents(as, aspect)
		}
		view.Summary["aspect_sentiment"] = as
	}
	view.Summary["override_count"] = len(overrides)
}

func adjustCountMap(m map[string]int, from, to string) {
	if from != "" {
		m[from]--
		if m[from] < 0 {
			m[from] = 0
		}
	}
	if to != "" {
		m[to]++
	}
}

func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return 0
	}
}

// adjustAspectSentimentCount — aspect_sentiment[aspect].sentiment[sentiment].count
// 를 delta만큼 조정(없으면 생성). total/percent는 recompute에서 다시 계산.
func adjustAspectSentimentCount(as map[string]any, aspect, sentiment string, delta int) {
	if aspect == "" || sentiment == "" {
		return
	}
	entry, ok := as[aspect].(map[string]any)
	if !ok {
		entry = map[string]any{"total": 0, "sentiment": map[string]any{}}
		as[aspect] = entry
	}
	sdist, ok := entry["sentiment"].(map[string]any)
	if !ok {
		sdist = map[string]any{}
		entry["sentiment"] = sdist
	}
	cell, ok := sdist[sentiment].(map[string]any)
	if !ok {
		cell = map[string]any{"count": 0, "percent": float64(0)}
		sdist[sentiment] = cell
	}
	c := asInt(cell["count"]) + delta
	if c < 0 {
		c = 0
	}
	cell["count"] = c
}

func recomputeAspectSentimentPercents(as map[string]any, aspect string) {
	entry, ok := as[aspect].(map[string]any)
	if !ok {
		return
	}
	sdist, ok := entry["sentiment"].(map[string]any)
	if !ok {
		return
	}
	total := 0
	for _, v := range sdist {
		if cell, ok := v.(map[string]any); ok {
			total += asInt(cell["count"])
		}
	}
	entry["total"] = total
	for _, v := range sdist {
		if cell, ok := v.(map[string]any); ok {
			cell["percent"] = percentOf(asInt(cell["count"]), total)
		}
	}
}
