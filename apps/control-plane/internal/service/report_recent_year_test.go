package service

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 기초보고서 "개요 외 섹션은 최신년도만" 필터 잠금 (silverone 2026-06-25).
// clean(created_at)에 doc_id JOIN으로 최신년도(MAX year)만 집계되는지, 날짜 없으면 전체로
// fallback하는지 검증한다.

// setupRecentYearFixture — clean parquet(row_id/created_at/source_json) + clause_label jsonl +
// clause_keywords jsonl + doc_genuineness jsonl을 임시 생성. withDate=false면 created_at을
// NULL로 + 날짜 후보 컬럼도 없게 해서 fallback 케이스를 만든다.
func setupRecentYearFixture(t *testing.T, withDate bool) (cleanRef, clauseRef, keywordRef, genRef string) {
	t.Helper()
	dir := t.TempDir()
	cleanRef = filepath.Join(dir, "clean.parquet")
	clauseRef = filepath.Join(dir, "clause_label.jsonl")
	keywordRef = filepath.Join(dir, "clause_keywords.jsonl")
	genRef = filepath.Join(dir, "doc_genuineness.jsonl")

	// docs: d1/d2 = 2025(최신), d3 = 2024. channel은 source_json에 보존.
	type doc struct {
		id   string
		year string
	}
	docs := []doc{{"d1", "2025-03-01"}, {"d2", "2025-07-15"}, {"d3", "2024-05-10"}}

	db, cleanup, err := openTempDuckDB()
	if err != nil {
		t.Fatalf("openTempDuckDB: %v", err)
	}
	defer cleanup()
	selects := make([]string, 0, len(docs))
	for _, d := range docs {
		createdAt := "'" + d.year + "'"
		if !withDate {
			createdAt = "NULL"
		}
		// source_json: 채널만(날짜 후보 컬럼은 넣지 않음 → withDate=false면 날짜 전무).
		// clean parquet은 row_id == doc_id (채널 집계가 clean.doc_id를 참조).
		selects = append(selects, fmt.Sprintf(
			"SELECT '%s' AS row_id, '%s' AS doc_id, %s::VARCHAR AS created_at, '{\"채널\":\"naver\"}' AS source_json",
			d.id, d.id, createdAt))
	}
	q := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", strings.Join(selects, " UNION ALL "), cleanRef)
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("write clean parquet: %v", err)
	}

	// clause_label: d1=2절, d2=1절, d3=3절 (전체 6, 최신년도 3).
	clauseLines := []string{
		`{"doc_id":"d1","clause":"맥주가 맛있다","sentiment":"positive","aspect":"food","source":"lloa","prompt_version":"v3"}`,
		`{"doc_id":"d1","clause":"가격이 비싸다","sentiment":"negative","aspect":"price","source":"lloa","prompt_version":"v3"}`,
		`{"doc_id":"d2","clause":"직원이 친절하다","sentiment":"positive","aspect":"service","source":"lloa","prompt_version":"v3"}`,
		`{"doc_id":"d3","clause":"불꽃놀이 좋았다","sentiment":"positive","aspect":"program","source":"lloa","prompt_version":"v3"}`,
		`{"doc_id":"d3","clause":"주차가 불편하다","sentiment":"negative","aspect":"facility","source":"lloa","prompt_version":"v3"}`,
		`{"doc_id":"d3","clause":"사람이 많다","sentiment":"neutral","aspect":"etc","source":"lloa","prompt_version":"v3"}`,
	}
	if err := os.WriteFile(clauseRef, []byte(strings.Join(clauseLines, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("write clause_label: %v", err)
	}

	// clause_keywords: 2025=맥주/가격/직원, 2024=불꽃/주차 (전체 unique 5, 최신년도 3).
	keywordLines := []string{
		`{"doc_id":"d1","clause_id":"d1__1","clause":"맥주가 맛있다","aspect":"food","sentiment":"positive","keyword":"맥주","source":"kiwi","extractor_version":"kiwi-noun-v2","keyword_rank_in_clause":1}`,
		`{"doc_id":"d1","clause_id":"d1__2","clause":"가격이 비싸다","aspect":"price","sentiment":"negative","keyword":"가격","source":"kiwi","extractor_version":"kiwi-noun-v2","keyword_rank_in_clause":1}`,
		`{"doc_id":"d2","clause_id":"d2__1","clause":"직원이 친절하다","aspect":"service","sentiment":"positive","keyword":"직원","source":"kiwi","extractor_version":"kiwi-noun-v2","keyword_rank_in_clause":1}`,
		`{"doc_id":"d3","clause_id":"d3__1","clause":"불꽃놀이 좋았다","aspect":"program","sentiment":"positive","keyword":"불꽃","source":"kiwi","extractor_version":"kiwi-noun-v2","keyword_rank_in_clause":1}`,
		`{"doc_id":"d3","clause_id":"d3__2","clause":"주차가 불편하다","aspect":"facility","sentiment":"negative","keyword":"주차","source":"kiwi","extractor_version":"kiwi-noun-v2","keyword_rank_in_clause":1}`,
	}
	if err := os.WriteFile(keywordRef, []byte(strings.Join(keywordLines, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("write clause_keywords: %v", err)
	}

	// doc_genuineness: 셋 다 genuine_review (전체 3, 최신년도 2).
	genLines := []string{
		`{"doc_id":"d1","genuineness":"genuine_review","reason":"r","source":"lloa","prompt_version":"v1"}`,
		`{"doc_id":"d2","genuineness":"genuine_review","reason":"r","source":"lloa","prompt_version":"v1"}`,
		`{"doc_id":"d3","genuineness":"genuine_review","reason":"r","source":"lloa","prompt_version":"v1"}`,
	}
	if err := os.WriteFile(genRef, []byte(strings.Join(genLines, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("write doc_genuineness: %v", err)
	}
	return cleanRef, clauseRef, keywordRef, genRef
}

func TestRecentYearFilter_ClauseLabel(t *testing.T) {
	cleanRef, clauseRef, _, _ := setupRecentYearFixture(t, true)

	// 필터 없음 → 전체 6절.
	whole, _, _, _, err := loadClauseLabelArtifact(clauseRef, "", 100, 0, "", "")
	if err != nil {
		t.Fatalf("whole: %v", err)
	}
	if got := asInt(whole["total"]); got != 6 {
		t.Fatalf("whole total = %d, want 6", got)
	}

	// 최신년도 필터 → 2025 doc(d1,d2)의 3절만.
	recent, _, _, _, err := loadClauseLabelArtifact(clauseRef, "", 100, 0, "", "", artifactRecentYearFilter{cleanRef: cleanRef})
	if err != nil {
		t.Fatalf("recent: %v", err)
	}
	if got := asInt(recent["total"]); got != 3 {
		t.Fatalf("recent total = %d, want 3", got)
	}
}

func TestRecentYearFilter_ClauseKeywords(t *testing.T) {
	cleanRef, _, keywordRef, _ := setupRecentYearFixture(t, true)

	whole, _, _, err := loadClauseKeywordsArtifact(keywordRef, 100, 0, "", "", "", "", nil)
	if err != nil {
		t.Fatalf("whole: %v", err)
	}
	if got := asInt(whole["unique_keyword_count"]); got != 5 {
		t.Fatalf("whole unique = %d, want 5", got)
	}

	recent, _, _, err := loadClauseKeywordsArtifact(keywordRef, 100, 0, "", "", "", "", nil, artifactRecentYearFilter{cleanRef: cleanRef})
	if err != nil {
		t.Fatalf("recent: %v", err)
	}
	// 2025: 맥주/가격/직원 = 3 (불꽃/주차 제외).
	if got := asInt(recent["unique_keyword_count"]); got != 3 {
		t.Fatalf("recent unique = %d, want 3", got)
	}
}

func TestRecentYearFilter_Channel(t *testing.T) {
	cleanRef, _, _, genRef := setupRecentYearFixture(t, true)

	whole, err := loadChannelGenuineBreakdown(cleanRef, genRef, false, false)
	if err != nil || whole == nil {
		t.Fatalf("whole: %v", err)
	}
	if got := asInt(whole["total"]); got != 3 {
		t.Fatalf("whole channel total = %d, want 3", got)
	}

	recent, err := loadChannelGenuineBreakdown(cleanRef, genRef, false, true)
	if err != nil || recent == nil {
		t.Fatalf("recent: %v", err)
	}
	if got := asInt(recent["total"]); got != 2 {
		t.Fatalf("recent channel total = %d, want 2", got)
	}
}

// 날짜 없으면 최신년도 필터는 no-op(전체) — 빈 보고서 방지.
func TestRecentYearFilter_NoDateFallsBackToWhole(t *testing.T) {
	cleanRef, clauseRef, _, _ := setupRecentYearFixture(t, false)

	recent, _, _, _, err := loadClauseLabelArtifact(clauseRef, "", 100, 0, "", "", artifactRecentYearFilter{cleanRef: cleanRef})
	if err != nil {
		t.Fatalf("recent: %v", err)
	}
	if got := asInt(recent["total"]); got != 6 {
		t.Fatalf("no-date fallback total = %d, want 6 (whole)", got)
	}
}
