package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode"

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

	// 키워드 정제 사전(silverone 2026-06-25) — dataset 단위 활성 규칙을 조회 overlay로
	// 적용(block 제외 / synonym 병합). 원본 artifact 불변. 규칙 없으면 no-op.
	rules, err := s.store.ListKeywordDictionaryRules(projectID, datasetID, true)
	if err != nil {
		return domain.DatasetArtifactView{}, err
	}

	summary, total, items, err := loadClauseKeywordsArtifact(ref, limit, offset, aspect, sentiment, q, group, rules)
	if err != nil {
		return domain.DatasetArtifactView{}, err
	}
	view.Summary = summary
	if n := countActiveKeywordRules(rules); n > 0 {
		view.Summary["dictionary_rule_count"] = n
	}
	// 추천 제외어 (silverone 2026-06-25) — 검색어/대상명(metadata.doc_genuineness)에서
	// 유래한 키워드를 결과 행에 표시한다. 자동 제외가 아니라 운영자가 [제외]로 승인하면
	// block 규칙이 된다(맥주/블루스처럼 핵심어일 수 있어 자동 block 금지). 키워드 집계
	// 행(group != clause)에만 단다.
	if dataset, derr := s.GetDataset(projectID, datasetID); derr == nil {
		if terms := subjectDerivedTerms(dataset); len(terms) > 0 {
			if n := annotateSuggestedExclude(items, terms); n > 0 {
				view.Summary["suggested_exclude_page_count"] = n
			}
			view.Summary["suggested_exclude_terms"] = sortedTermList(terms)
		}
	}
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
func loadClauseKeywordsArtifact(ref string, limit, offset int, aspect, sentiment, q, group string, rules []domain.KeywordDictionaryRule, filters ...artifactRecentYearFilter) (map[string]any, int, []map[string]any, error) {
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		return nil, 0, nil, err
	}
	defer cleanup()

	// 사전 적용 source — 활성 규칙이 없으면 평범한 read_json. 모든 하위 집계가
	// 이 source 위에서 돌아 block 제외/synonym 병합이 재집계까지 자동 반영된다.
	source := buildKeywordDictionarySource(ref, rules)
	// 최신년도 섹션 필터(silverone 2026-06-25) — source를 최신년도 doc로 좁힌다(없으면 no-op).
	source, _ = wrapSourceRecentYear(db, source, filters)

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

// loadClauseGroupedKeywords — 절 중심 item table. SNS 리포스트/복붙으로 같은 절 텍스트가
// 여러 doc에 반복되므로, clause_id가 아니라 **절 텍스트로 묶어 중복 제거**한다(고유 절만).
// occurrence_count로 그 절이 몇 번 등장했는지 함께 준다. 절(clause)마다 추출된 키워드를
// 배열로 묶어 {clause, keywords[], occurrence_count}로 돌려준다. 키워드는 절에서 뽑힌
// 순서(keyword_rank_in_clause)대로 정렬한다(프론트 하이라이트가 절 위치와 맞도록).
// q(절 본문 또는 키워드 부분일치)로 절을 거르고, 키워드 많은 절 우선 정렬.
func loadClauseGroupedKeywords(db *sql.DB, source string, limit, offset int, q string) (int, []map[string]any, error) {
	clauseFilter := ""
	if term := strings.TrimSpace(q); term != "" {
		esc := escapeDuckDBLiteral(term)
		clauseFilter = fmt.Sprintf(
			"AND clause IN (SELECT DISTINCT clause FROM %s WHERE keyword IS NOT NULL AND (clause ILIKE '%%%s%%' OR keyword ILIKE '%%%s%%'))",
			source, esc, esc)
	}

	// 고유 절 텍스트 수(중복 제거 후).
	total, err := scalarCount(db, fmt.Sprintf(
		"SELECT COUNT(DISTINCT clause) FROM %s WHERE keyword IS NOT NULL %s",
		source, clauseFilter))
	if err != nil {
		return 0, nil, err
	}

	// base: 절 텍스트 dedup 전 행. kw: 절×키워드 dedup(중복 절의 같은 키워드는 최소 rank로).
	// keywords는 list(... ORDER BY rk)로 절 등장 순서 유지.
	itemQuery := fmt.Sprintf(
		`WITH base AS (
		    SELECT clause, keyword, clause_id, keyword_rank_in_clause AS rk
		    FROM %[1]s
		    WHERE keyword IS NOT NULL %[2]s
		 ),
		 kw AS (
		    SELECT clause, keyword, MIN(rk) AS rk FROM base GROUP BY clause, keyword
		 ),
		 occ AS (
		    SELECT clause, COUNT(DISTINCT clause_id) AS occurrence_count FROM base GROUP BY clause
		 ),
		 grouped AS (
		    SELECT clause,
		           CAST(to_json(list(keyword ORDER BY rk, keyword)) AS VARCHAR) AS keywords,
		           COUNT(*) AS keyword_count
		    FROM kw GROUP BY clause
		 )
		 SELECT g.clause, g.keywords, o.occurrence_count
		 FROM grouped g JOIN occ o ON g.clause = o.clause
		 ORDER BY g.keyword_count DESC, g.clause
		 LIMIT %[3]d OFFSET %[4]d`,
		source, clauseFilter, limit, offset,
	)
	rows, err := scanArtifactRows(db, itemQuery, []string{"clause", "keywords", "occurrence_count"})
	if err != nil {
		return 0, nil, err
	}
	items := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		items = append(items, map[string]any{
			"clause":           r["clause"],
			"keywords":         decodeStringArray(r["keywords"]),
			"occurrence_count": r["occurrence_count"],
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

// subjectDerivedTerms — dataset.metadata.doc_genuineness의 subject_name +
// subject_aliases + recruitment_keywords를 구분자(공백/&/·/구두점)로 쪼갠 토큰 집합.
// "추천 제외어" 매칭용 — 키워드가 검색어/대상명에서 유래했는지 판별한다. 2글자 미만
// 토큰은 노이즈 매칭 방지로 버린다(extractor min_len=2와 정합). 매칭은 소문자 정규화.
func subjectDerivedTerms(dataset domain.Dataset) map[string]bool {
	raw, _ := dataset.Metadata["doc_genuineness"].(map[string]any)
	if raw == nil {
		return nil
	}
	parts := []string{anyStringValue(raw["subject_name"])}
	parts = append(parts, anyStringList(raw["subject_aliases"])...)
	parts = append(parts, anyStringList(raw["recruitment_keywords"])...)
	isSep := func(r rune) bool {
		switch r {
		case '&', '/', ',', '·', '・', '|', '+', '(', ')', '[', ']', '-', '_', '~':
			return true
		}
		return unicode.IsSpace(r)
	}
	terms := map[string]bool{}
	for _, p := range parts {
		for _, tok := range strings.FieldsFunc(p, isSep) {
			t := strings.ToLower(strings.TrimSpace(tok))
			if len([]rune(t)) >= 2 {
				terms[t] = true
			}
		}
	}
	if len(terms) == 0 {
		return nil
	}
	return terms
}

// annotateSuggestedExclude — 키워드 집계 item("keyword" 보유)에 검색어 유래 플래그를
// 단다. 현재 페이지 행만 대상(페이징 무관 전체는 summary의 suggested_exclude_terms 참고).
// 반환: 이 페이지에서 플래그된 행 수.
func annotateSuggestedExclude(items []map[string]any, terms map[string]bool) int {
	if len(terms) == 0 {
		return 0
	}
	count := 0
	for _, item := range items {
		kw, ok := item["keyword"].(string)
		if !ok {
			continue
		}
		if terms[strings.ToLower(strings.TrimSpace(kw))] {
			item["suggested_exclude"] = true
			count++
		}
	}
	return count
}

// sortedTermList — term set을 정렬된 슬라이스로(프론트 "검색어 유래 후보" 칩 표시용).
func sortedTermList(terms map[string]bool) []string {
	out := make([]string, 0, len(terms))
	for t := range terms {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}
