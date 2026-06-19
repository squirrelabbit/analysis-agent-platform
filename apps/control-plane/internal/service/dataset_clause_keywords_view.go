package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

const (
	// 전체 긍/부 카드용 상위 키워드 수.
	topKeywordCardN = 5
	// aspect별 긍/부 워드클라우드용 상위 키워드 수. aspect 9 × sentiment 2 × 30 = 최대 540 rows.
	wordCloudTopN = 30
)

// GetClauseKeywordsView — clause_keywords artifact 화면/대시보드용 응답 (silverone 2026-06-10).
//
// long-format(절-키워드 1행) artifact를 DuckDB on-demand 집계로 읽어 dashboard
// summary + 필터/페이징된 item table을 만든다. 별도 저장 구조 없이 read_json 집계.
// clause_keywords가 없는 version은 status만 채운 빈 view를 돌려준다(analyze 흐름과 무관).
// group=="clause"면 item table을 키워드 중심이 아니라 절 중심
// ({clause, keywords[]})으로 돌려준다 (silverone 2026-06-19, "절에서 추출된 키워드" 표).
func (s *DatasetService) GetClauseKeywordsView(
	projectID, datasetID, datasetVersionID string,
	limit, offset int,
	aspect, sentiment, q, group string,
) (domain.DatasetArtifactView, error) {
	version, err := s.GetDatasetVersion(projectID, datasetID, datasetVersionID)
	if err != nil {
		return domain.DatasetArtifactView{}, err
	}
	limit, offset = normalizeArtifactPagination(limit, offset)

	view := domain.DatasetArtifactView{
		BuildType: datasetBuildTypeClauseKeywords,
		Items:     []map[string]any{},
		Pagination: &domain.ArtifactPagination{
			Limit:  limit,
			Offset: offset,
		},
	}

	latestJob := latestJobForBuildType(s, projectID, version.DatasetVersionID, datasetBuildTypeClauseKeywords)
	ref := strings.TrimSpace(metadataString(version.Metadata, "clause_keywords_ref", ""))
	if ref == "" {
		ref = strings.TrimSpace(metadataString(version.Metadata, "clause_keywords_uri", ""))
	}
	view.Status = resolveArtifactStatus(ref, latestJob, metadataString(version.Metadata, "clause_keywords_status", ""))
	enrichViewWithJob(&view, latestJob, version.Metadata, datasetBuildTypeClauseKeywords)

	if !artifactReadyForView(ref) {
		return view, nil
	}
	if _, err := os.Stat(ref); err != nil {
		if os.IsNotExist(err) {
			return view, nil
		}
		return domain.DatasetArtifactView{}, err
	}

	summary, total, items, err := loadClauseKeywordsArtifact(ref, limit, offset, aspect, sentiment, q, group)
	if err != nil {
		return domain.DatasetArtifactView{}, err
	}
	view.Summary = summary
	// applied: 빌드 당시 extractor_version (per-row에도 있지만 summary metadata에서 회수).
	if ev := summaryMetadataString(version.Metadata, "clause_keywords_summary", "extractor_version"); ev != "" {
		view.Applied = map[string]any{"extractor_version": ev}
	}
	view.Items = items
	view.Pagination.Total = total
	return view, nil
}

// loadClauseKeywordsArtifact — clause_keywords long-format jsonl을 DuckDB로 집계.
// 반환: dashboard summary / 필터 적용 total(페이징용) / 페이징된 item rows.
func loadClauseKeywordsArtifact(ref string, limit, offset int, aspect, sentiment, q, group string) (map[string]any, int, []map[string]any, error) {
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		return nil, 0, nil, err
	}
	defer cleanup()

	source := fmt.Sprintf("read_json('%s', format='newline_delimited')", escapeDuckDBLiteral(ref))

	// ── dashboard summary (필터 미적용 전체) ──────────────────────────────
	total, byAspect, err := aggregateGroupedCounts(db, source, "aspect")
	if err != nil {
		return nil, 0, nil, err
	}
	_, bySentiment, err := aggregateGroupedCounts(db, source, "sentiment")
	if err != nil {
		return nil, 0, nil, err
	}
	uniqueKeywords, err := scalarCount(db, fmt.Sprintf("SELECT COUNT(DISTINCT keyword) FROM %s WHERE keyword IS NOT NULL", source))
	if err != nil {
		return nil, 0, nil, err
	}
	clausesWithKeywords, err := scalarCount(db, fmt.Sprintf("SELECT COUNT(DISTINCT clause_id) FROM %s", source))
	if err != nil {
		return nil, 0, nil, err
	}
	// 전체 카드용 top5 (긍/부).
	topPositive, err := topKeywordsBySentiment(db, source, "positive", topKeywordCardN)
	if err != nil {
		return nil, 0, nil, err
	}
	topNegative, err := topKeywordsBySentiment(db, source, "negative", topKeywordCardN)
	if err != nil {
		return nil, 0, nil, err
	}
	// aspect × sentiment(긍/부) 워드클라우드용 top30 + weight(0~1). query 무관 전체.
	aspectSentimentKeywords, err := aggregateAspectSentimentKeywords(db, source, wordCloudTopN)
	if err != nil {
		return nil, 0, nil, err
	}

	summary := map[string]any{
		// silverone 2026-06-10 — 프론트 친화 naming(_count 통일).
		"total_keyword_count":   total,               // 총 언급(절-키워드 행) 수
		"unique_keyword_count":  uniqueKeywords,      // 고유 키워드 수
		"clause_count":          clausesWithKeywords, // 키워드가 1개 이상인 절 수(distinct clause_id)
		"aspect":                byAspect,
		"sentiment":             bySentiment,
		"top_keywords_positive": topPositive, // 전체 카드용 5개
		"top_keywords_negative": topNegative,
		// aspect별 워드클라우드용 — {aspect: {positive:[{keyword,count,weight}…30], negative:[…]}}.
		"aspect_sentiment_keywords": aspectSentimentKeywords,
	}

	// 선택된 aspect의 sentiment 분포 (aspect 필터가 있을 때만). 대시보드에서
	// aspect를 클릭하면 그 aspect 안의 긍/중/부 분포를 본다.
	if a := strings.TrimSpace(aspect); a != "" {
		selWhere := fmt.Sprintf("WHERE aspect = '%s'", escapeDuckDBLiteral(a))
		selTotal, selSentiment, err := aggregateGroupedCountsWhere(db, source, "sentiment", selWhere)
		if err != nil {
			return nil, 0, nil, err
		}
		summary["selected_aspect"] = a
		summary["selected_aspect_total"] = selTotal
		summary["selected_aspect_sentiment"] = selSentiment
	}

	// ── group=clause: item table을 절 중심({clause, keywords[]})으로 ──────────
	// "절에서 추출된 키워드" 표용. aspect/sentiment 필터는 절표에서 안 쓰고 q(절·키워드
	// 부분일치)만 적용한다. 매칭되는 절은 그 절의 전체 키워드를 함께 보여준다.
	if strings.TrimSpace(group) == "clause" {
		clauseTotal, clauseItems, err := loadClauseGroupedKeywords(db, source, limit, offset, q)
		if err != nil {
			return nil, 0, nil, err
		}
		return summary, clauseTotal, clauseItems, nil
	}

	// ── item table = 키워드 집계 행 (필터 적용 후 keyword GROUP BY) ───────────
	// 프론트 1차 화면은 raw 절-키워드가 아니라 "키워드별: 언급수/문서수/우세감성/
	// 대표 aspect/대표 절". 필터(aspect/sentiment/q)는 집계 *전* 행에 적용하고,
	// 페이징은 키워드 단위. dominant_*/top_aspect는 mode(arg_max)로 산출.
	where := buildKeywordFilter(aspect, sentiment, q)
	filteredTotal, err := scalarCount(db, fmt.Sprintf(
		"SELECT COUNT(DISTINCT keyword) FROM %s %s%skeyword IS NOT NULL",
		source, where, whereGlue(where)))
	if err != nil {
		return nil, 0, nil, err
	}

	itemQuery := fmt.Sprintf(
		`WITH filtered AS (
		    SELECT * FROM %s %s%skeyword IS NOT NULL
		 ),
		 ks AS (SELECT keyword, sentiment, COUNT(*) c FROM filtered GROUP BY keyword, sentiment),
		 ka AS (SELECT keyword, aspect, COUNT(*) c FROM filtered GROUP BY keyword, aspect),
		 base AS (
		    SELECT keyword,
		           COUNT(*) AS count,
		           COUNT(DISTINCT doc_id) AS document_count,
		           arg_max(clause, length(clause)) AS representative_clause
		    FROM filtered GROUP BY keyword
		 ),
		 dom_sent AS (SELECT keyword, arg_max(sentiment, c) AS dominant_sentiment, MAX(c) AS dom_c FROM ks GROUP BY keyword),
		 dom_asp  AS (SELECT keyword, arg_max(aspect, c) AS top_aspect FROM ka GROUP BY keyword)
		 SELECT b.keyword,
		        b.count,
		        b.document_count,
		        s.dominant_sentiment,
		        ROUND(CAST(s.dom_c AS DOUBLE) / b.count, 4) AS dominant_sentiment_ratio,
		        a.top_aspect,
		        b.representative_clause
		 FROM base b
		 JOIN dom_sent s USING (keyword)
		 JOIN dom_asp a USING (keyword)
		 ORDER BY b.count DESC, b.keyword
		 LIMIT %d OFFSET %d`,
		source, where, whereGlue(where), limit, offset,
	)
	items, err := scanArtifactRows(db, itemQuery, []string{
		"keyword", "count", "document_count", "dominant_sentiment",
		"dominant_sentiment_ratio", "top_aspect", "representative_clause",
	})
	if err != nil {
		return nil, 0, nil, err
	}
	return summary, filteredTotal, items, nil
}

// loadClauseGroupedKeywords — 절 중심 item table. 절(clause_id)마다 추출된 키워드를
// 배열로 묶어 {clause, keywords[]}로 돌려준다. q(절 본문 또는 키워드 부분일치)로 절을
// 거르되, 매칭된 절은 그 절의 전체 키워드를 보여준다. 키워드 많은 절 우선 정렬.
func loadClauseGroupedKeywords(db *sql.DB, source string, limit, offset int, q string) (int, []map[string]any, error) {
	clauseFilter := ""
	if term := strings.TrimSpace(q); term != "" {
		esc := escapeDuckDBLiteral(term)
		clauseFilter = fmt.Sprintf(
			"AND clause_id IN (SELECT DISTINCT clause_id FROM %s WHERE keyword IS NOT NULL AND (clause ILIKE '%%%s%%' OR keyword ILIKE '%%%s%%'))",
			source, esc, esc)
	}

	total, err := scalarCount(db, fmt.Sprintf(
		"SELECT COUNT(DISTINCT clause_id) FROM %s WHERE keyword IS NOT NULL %s",
		source, clauseFilter))
	if err != nil {
		return 0, nil, err
	}

	itemQuery := fmt.Sprintf(
		`WITH grouped AS (
		    SELECT clause_id,
		           any_value(clause) AS clause,
		           CAST(to_json(list(DISTINCT keyword)) AS VARCHAR) AS keywords,
		           COUNT(DISTINCT keyword) AS keyword_count
		    FROM %s
		    WHERE keyword IS NOT NULL %s
		    GROUP BY clause_id
		 )
		 SELECT clause, keywords
		 FROM grouped
		 ORDER BY keyword_count DESC, clause
		 LIMIT %d OFFSET %d`,
		source, clauseFilter, limit, offset,
	)
	rows, err := scanArtifactRows(db, itemQuery, []string{"clause", "keywords"})
	if err != nil {
		return 0, nil, err
	}
	items := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		items = append(items, map[string]any{
			"clause":   r["clause"],
			"keywords": decodeStringArray(r["keywords"]),
		})
	}
	return total, items, nil
}

// decodeStringArray — DuckDB to_json(list(...)) VARCHAR(JSON 배열 문자열)을 []string으로.
// 파싱 실패/빈 값이면 빈 배열.
func decodeStringArray(v any) []string {
	s, ok := v.(string)
	if !ok || strings.TrimSpace(s) == "" {
		return []string{}
	}
	var out []string
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return []string{}
	}
	return out
}

// whereGlue — 기존 WHERE 절에 추가 조건(keyword IS NOT NULL)을 잇는 연결어.
// where가 비면 "WHERE ", 있으면 " AND " 를 돌려준다.
func whereGlue(where string) string {
	if strings.TrimSpace(where) == "" {
		return "WHERE "
	}
	return " AND "
}

// buildKeywordFilter — aspect/sentiment/q 필터 WHERE 절. q는 keyword/clause 부분일치.
func buildKeywordFilter(aspect, sentiment, q string) string {
	conds := make([]string, 0, 3)
	if a := strings.TrimSpace(aspect); a != "" {
		conds = append(conds, fmt.Sprintf("aspect = '%s'", escapeDuckDBLiteral(a)))
	}
	if s := strings.TrimSpace(sentiment); s != "" {
		conds = append(conds, fmt.Sprintf("sentiment = '%s'", escapeDuckDBLiteral(s)))
	}
	if term := strings.TrimSpace(q); term != "" {
		esc := escapeDuckDBLiteral(term)
		conds = append(conds, fmt.Sprintf("(keyword ILIKE '%%%s%%' OR clause ILIKE '%%%s%%')", esc, esc))
	}
	if len(conds) == 0 {
		return ""
	}
	return "WHERE " + strings.Join(conds, " AND ")
}

// topKeywordsBySentiment — 특정 sentiment에서 빈도 상위 N 키워드 [{keyword,count}].
func topKeywordsBySentiment(db *sql.DB, source, sentiment string, n int) ([]map[string]any, error) {
	query := fmt.Sprintf(
		`SELECT keyword, COUNT(*) AS count
		 FROM %s
		 WHERE sentiment = '%s' AND keyword IS NOT NULL
		 GROUP BY keyword
		 ORDER BY count DESC, keyword
		 LIMIT %d`,
		source, escapeDuckDBLiteral(sentiment), n,
	)
	rows, err := scanArtifactRows(db, query, []string{"keyword", "count"})
	if err != nil {
		return nil, err
	}
	if rows == nil {
		rows = []map[string]any{}
	}
	return rows, nil
}

// aggregateAspectSentimentKeywords — aspect × sentiment(positive/negative)별 상위 topN
// 키워드를 워드클라우드용으로 집계한다. weight = count / (그 aspect·sentiment 내 최대 count)
// 으로 0~1 정규화(최상위=1.0). neutral은 워드클라우드 활용성이 낮아 제외. 결과는
// {aspect: {sentiment: [{keyword,count,weight}…]}} 중첩 map.
func aggregateAspectSentimentKeywords(db *sql.DB, source string, topN int) (map[string]map[string][]map[string]any, error) {
	query := fmt.Sprintf(
		`WITH counts AS (
		    SELECT aspect, sentiment, keyword, COUNT(*) AS c
		    FROM %s
		    WHERE sentiment IN ('positive', 'negative') AND keyword IS NOT NULL
		    GROUP BY aspect, sentiment, keyword
		 ),
		 ranked AS (
		    SELECT aspect, sentiment, keyword, c,
		           ROW_NUMBER() OVER (PARTITION BY aspect, sentiment ORDER BY c DESC, keyword) AS rn,
		           MAX(c) OVER (PARTITION BY aspect, sentiment) AS maxc
		    FROM counts
		 )
		 SELECT aspect, sentiment, keyword, c AS count,
		        ROUND(CAST(c AS DOUBLE) / maxc, 4) AS weight
		 FROM ranked
		 WHERE rn <= %d
		 ORDER BY aspect, sentiment, c DESC, keyword`,
		source, topN,
	)
	rows, err := scanArtifactRows(db, query, []string{"aspect", "sentiment", "keyword", "count", "weight"})
	if err != nil {
		return nil, err
	}
	out := map[string]map[string][]map[string]any{}
	for _, r := range rows {
		aspect := fmt.Sprint(r["aspect"])
		sentiment := fmt.Sprint(r["sentiment"])
		if out[aspect] == nil {
			out[aspect] = map[string][]map[string]any{}
		}
		out[aspect][sentiment] = append(out[aspect][sentiment], map[string]any{
			"keyword": r["keyword"],
			"count":   r["count"],
			"weight":  r["weight"],
		})
	}
	return out, nil
}

// aggregateGroupedCountsWhere — where 조건 적용 후 group_by 컬럼별 count + total.
func aggregateGroupedCountsWhere(db *sql.DB, source, groupColumn, where string) (int, map[string]int, error) {
	result := map[string]int{}
	query := fmt.Sprintf(`SELECT %s AS grp, COUNT(*) AS cnt FROM %s %s GROUP BY %s`,
		groupColumn, source, where, groupColumn)
	rows, err := db.Query(query)
	if err != nil {
		return 0, nil, err
	}
	defer rows.Close()
	total := 0
	for rows.Next() {
		var key sql.NullString
		var cnt int
		if err := rows.Scan(&key, &cnt); err != nil {
			return 0, nil, err
		}
		result[normalizeArtifactKey(key)] = cnt
		total += cnt
	}
	if err := rows.Err(); err != nil {
		return 0, nil, err
	}
	return total, result, nil
}

// scalarCount — 단일 정수 결과 쿼리.
func scalarCount(db *sql.DB, query string) (int, error) {
	var n int
	if err := db.QueryRow(query).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
