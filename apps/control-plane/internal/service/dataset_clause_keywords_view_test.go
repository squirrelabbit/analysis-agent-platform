package service

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"analysis-support-platform/control-plane/internal/domain"
)

func asFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case int:
		return float64(n)
	case string:
		f, _ := strconv.ParseFloat(n, 64)
		return f
	}
	return -1
}

// silverone 2026-06-10 — clause_keywords 대시보드 view 집계/필터/페이징 잠금.
// long-format(절-키워드 1행) artifact 기준.
func setupClauseKeywordsFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "clause_keywords.jsonl")
	lines := []string{
		`{"doc_id":"d1","clause_id":"d1__1","clause":"푸드트럭 가격이 비쌈","aspect":"food","sentiment":"negative","keyword":"가격","keyword_rank_in_clause":1,"source":"kiwi","extractor_version":"kiwi-noun-v2"}`,
		`{"doc_id":"d1","clause_id":"d1__1","clause":"푸드트럭 가격이 비쌈","aspect":"food","sentiment":"negative","keyword":"푸드트럭","keyword_rank_in_clause":2,"source":"kiwi","extractor_version":"kiwi-noun-v2"}`,
		`{"doc_id":"d1","clause_id":"d1__2","clause":"직원 친절","aspect":"service","sentiment":"positive","keyword":"친절","keyword_rank_in_clause":1,"source":"kiwi","extractor_version":"kiwi-noun-v2"}`,
		`{"doc_id":"d2","clause_id":"d2__1","clause":"맛 좋음","aspect":"food","sentiment":"positive","keyword":"맛","keyword_rank_in_clause":1,"source":"kiwi","extractor_version":"kiwi-noun-v2"}`,
		`{"doc_id":"d2","clause_id":"d2__2","clause":"가격 부담","aspect":"food","sentiment":"negative","keyword":"가격","keyword_rank_in_clause":1,"source":"kiwi","extractor_version":"kiwi-noun-v2"}`,
		`{"doc_id":"d3","clause_id":"d3__1","clause":"드론쇼","aspect":"program","sentiment":"neutral","keyword":"드론","keyword_rank_in_clause":1,"source":"kiwi","extractor_version":"kiwi-noun-v2"}`,
	}
	if err := os.WriteFile(path, []byte(joinLines(lines)), 0o644); err != nil {
		t.Fatalf("write jsonl: %v", err)
	}
	return path
}

func TestLoadClauseKeywords_SummaryKPI(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	summary, total, items, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "", "", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// items는 키워드 집계 행 → distinct keyword = 5 (가격(2)/푸드트럭/친절/맛/드론).
	if total != 5 || len(items) != 5 {
		t.Fatalf("total=%d items=%d, want 5/5 (distinct keywords)", total, len(items))
	}
	if summary["total_keyword_count"] != 6 { // 총 언급(행) 수
		t.Fatalf("total_keyword_count=%v, want 6", summary["total_keyword_count"])
	}
	if summary["unique_keyword_count"] != 5 {
		t.Fatalf("unique_keyword_count=%v, want 5", summary["unique_keyword_count"])
	}
	if summary["clause_count"] != 5 {
		t.Fatalf("clause_count=%v, want 5", summary["clause_count"])
	}
}

func TestLoadClauseKeywords_ItemColumns(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	_, _, items, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "", "", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	var price map[string]any
	for _, it := range items {
		if fmt.Sprint(it["keyword"]) == "가격" {
			price = it
		}
	}
	if price == nil {
		t.Fatalf("가격 keyword row missing: %v", items)
	}
	// 가격: 언급 2(d1__1, d2__2), 문서 2, 우세감성 negative(2/2=1.0), top_aspect food.
	if fmt.Sprint(price["count"]) != "2" {
		t.Fatalf("가격.count=%v, want 2", price["count"])
	}
	if fmt.Sprint(price["document_count"]) != "2" {
		t.Fatalf("가격.document_count=%v, want 2", price["document_count"])
	}
	if fmt.Sprint(price["dominant_sentiment"]) != "negative" {
		t.Fatalf("가격.dominant_sentiment=%v, want negative", price["dominant_sentiment"])
	}
	if fmt.Sprint(price["dominant_sentiment_ratio"]) != "1" {
		t.Fatalf("가격.dominant_sentiment_ratio=%v, want 1", price["dominant_sentiment_ratio"])
	}
	if fmt.Sprint(price["top_aspect"]) != "food" {
		t.Fatalf("가격.top_aspect=%v, want food", price["top_aspect"])
	}
	if fmt.Sprint(price["representative_clause"]) == "" {
		t.Fatalf("가격.representative_clause empty")
	}
}

func TestLoadClauseKeywords_AspectSummary(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	summary, _, _, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "", "", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	aspect, ok := summary["aspect"].(map[string]int)
	if !ok {
		t.Fatalf("summary.aspect type=%T", summary["aspect"])
	}
	if aspect["food"] != 4 || aspect["service"] != 1 || aspect["program"] != 1 {
		t.Fatalf("aspect=%v, want food4/service1/program1", aspect)
	}
}

func TestLoadClauseKeywords_SelectedAspectSentiment(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	// aspect=food 선택 → 그 aspect 안의 sentiment 분포.
	summary, _, _, err := loadClauseKeywordsArtifact(path, 100, 0, "food", "", "", "", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if summary["selected_aspect"] != "food" {
		t.Fatalf("selected_aspect=%v, want food", summary["selected_aspect"])
	}
	if summary["selected_aspect_total"] != 4 {
		t.Fatalf("selected_aspect_total=%v, want 4", summary["selected_aspect_total"])
	}
	sel, ok := summary["selected_aspect_sentiment"].(map[string]int)
	if !ok {
		t.Fatalf("selected_aspect_sentiment type=%T", summary["selected_aspect_sentiment"])
	}
	if sel["negative"] != 3 || sel["positive"] != 1 { // food: 가격,푸드트럭,가격(neg) + 맛(pos)
		t.Fatalf("selected_aspect_sentiment=%v, want neg3/pos1", sel)
	}
}

func TestLoadClauseKeywords_TopKeywords(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	summary, _, _, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "", "", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	neg, ok := summary["top_keywords_negative"].([]map[string]any)
	if !ok || len(neg) == 0 {
		t.Fatalf("top_keywords_negative=%v", summary["top_keywords_negative"])
	}
	// 가격이 부정에서 2회로 1위.
	if fmt.Sprint(neg[0]["keyword"]) != "가격" || fmt.Sprint(neg[0]["count"]) != "2" {
		t.Fatalf("top negative[0]=%v, want 가격/2", neg[0])
	}
	pos, ok := summary["top_keywords_positive"].([]map[string]any)
	if !ok || len(pos) != 2 { // 맛, 친절
		t.Fatalf("top_keywords_positive=%v, want 2", summary["top_keywords_positive"])
	}
}

func TestLoadClauseKeywords_Filters(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	// aspect=food + sentiment=negative → 행 가격,푸드트럭,가격 → distinct keyword 2(가격,푸드트럭).
	_, total, items, err := loadClauseKeywordsArtifact(path, 100, 0, "food", "negative", "", "", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("food+negative total=%d items=%d, want 2/2 (distinct keyword)", total, len(items))
	}
	// 가격은 negative-food에서 2회 → count 2로 1위.
	if fmt.Sprint(items[0]["keyword"]) != "가격" || fmt.Sprint(items[0]["count"]) != "2" {
		t.Fatalf("items[0]=%v, want 가격/2", items[0])
	}
}

func TestLoadClauseKeywords_QFilter(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	// q=맛 → keyword '맛' 또는 clause '맛 좋음' 부분일치 → keyword 맛 1종.
	_, total, items, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "맛", "", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("q=맛 total=%d items=%d, want 1/1", total, len(items))
	}
	if fmt.Sprint(items[0]["keyword"]) != "맛" {
		t.Fatalf("q=맛 item=%v", items[0])
	}
}

func TestLoadClauseKeywords_Pagination(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	_, total, items, err := loadClauseKeywordsArtifact(path, 2, 0, "", "", "", "", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if total != 5 { // distinct keyword 총수
		t.Fatalf("total=%d, want 5 (distinct keywords)", total)
	}
	if len(items) != 2 {
		t.Fatalf("limit=2 → items=%d, want 2", len(items))
	}
}

func TestLoadClauseKeywords_AspectSentimentKeywords(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	summary, _, _, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "", "", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	ask, ok := summary["aspect_sentiment_keywords"].(map[string]map[string][]map[string]any)
	if !ok {
		t.Fatalf("aspect_sentiment_keywords type=%T", summary["aspect_sentiment_keywords"])
	}
	// food: positive(맛), negative(가격2, 푸드트럭1) 모두 존재.
	food, ok := ask["food"]
	if !ok {
		t.Fatalf("aspect food missing: %v", ask)
	}
	if len(food["positive"]) == 0 || len(food["negative"]) == 0 {
		t.Fatalf("food pos/neg list missing: %v", food)
	}
	// negative count desc + weight 정규화(최상위 1.0).
	neg := food["negative"]
	if fmt.Sprint(neg[0]["keyword"]) != "가격" || fmt.Sprint(neg[0]["count"]) != "2" {
		t.Fatalf("food.negative[0]=%v, want 가격/2", neg[0])
	}
	if asFloat(neg[0]["weight"]) != 1.0 {
		t.Fatalf("top weight=%v, want 1.0", neg[0]["weight"])
	}
	if len(neg) >= 2 && asFloat(neg[1]["weight"]) != 0.5 { // 푸드트럭 1/2
		t.Fatalf("second weight=%v, want 0.5", neg[1]["weight"])
	}
	// count desc + weight 0~1 전수 검증.
	for asp, sents := range ask {
		for sent, list := range sents {
			if sent == "neutral" {
				t.Fatalf("neutral must be excluded, got %s/%s", asp, sent)
			}
			prev := 1 << 30
			for _, kw := range list {
				c, _ := strconv.Atoi(fmt.Sprint(kw["count"]))
				if c > prev {
					t.Fatalf("%s/%s not count-desc: %v", asp, sent, list)
				}
				prev = c
				w := asFloat(kw["weight"])
				if w <= 0 || w > 1.0 {
					t.Fatalf("weight out of (0,1]: %v in %s/%s", kw["weight"], asp, sent)
				}
			}
		}
	}
}

func TestLoadClauseKeywords_WordCloudCap30(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ck.jsonl")
	var lines []string
	for i := 0; i < 40; i++ { // food/negative에 고유 키워드 40개
		lines = append(lines, fmt.Sprintf(
			`{"doc_id":"d%d","clause_id":"d%d__1","clause":"c","aspect":"food","sentiment":"negative","keyword":"kw%02d","keyword_rank_in_clause":1,"source":"kiwi","extractor_version":"v"}`,
			i, i, i))
	}
	if err := os.WriteFile(path, []byte(joinLines(lines)), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	summary, _, _, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "", "", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	ask := summary["aspect_sentiment_keywords"].(map[string]map[string][]map[string]any)
	if got := len(ask["food"]["negative"]); got != 30 {
		t.Fatalf("word cloud list = %d, want 30 (cap)", got)
	}
}

func TestLoadClauseKeywords_TopCardCappedAt5(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ck.jsonl")
	var lines []string
	for i := 0; i < 8; i++ { // 부정 고유 키워드 8개 → 카드는 5개로 제한.
		lines = append(lines, fmt.Sprintf(
			`{"doc_id":"d%d","clause_id":"d%d__1","clause":"c","aspect":"food","sentiment":"negative","keyword":"kw%d","keyword_rank_in_clause":1,"source":"kiwi","extractor_version":"v"}`,
			i, i, i))
	}
	if err := os.WriteFile(path, []byte(joinLines(lines)), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	summary, _, _, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "", "", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	neg := summary["top_keywords_negative"].([]map[string]any)
	if len(neg) != 5 {
		t.Fatalf("top_keywords_negative = %d, want 5 (card cap)", len(neg))
	}
}

// silverone 2026-06-19 — group=clause: "절에서 추출된 키워드" 표용 절 중심 집계.
// 절(clause_id)마다 {clause, keywords[]}. 키워드 많은 절 우선.
func TestLoadClauseKeywords_ClauseGroup(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	_, total, items, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "", "clause", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// distinct clause = 5 (d1__1/d1__2/d2__1/d2__2/d3__1).
	if total != 5 || len(items) != 5 {
		t.Fatalf("clause group total=%d items=%d, want 5/5", total, len(items))
	}
	// 키워드 2개인 d1__1 "푸드트럭 가격이 비쌈"이 먼저(keyword_count DESC).
	first := items[0]
	if fmt.Sprint(first["clause"]) != "푸드트럭 가격이 비쌈" {
		t.Fatalf("first clause = %v, want 푸드트럭 가격이 비쌈", first["clause"])
	}
	kws, ok := first["keywords"].([]string)
	if !ok {
		t.Fatalf("keywords type = %T, want []string", first["keywords"])
	}
	if len(kws) != 2 {
		t.Fatalf("first clause keywords = %v, want 2 (가격·푸드트럭)", kws)
	}
	// 키워드는 절에서 뽑힌 순서(keyword_rank_in_clause)대로: 가격(rank1) → 푸드트럭(rank2).
	if kws[0] != "가격" || kws[1] != "푸드트럭" {
		t.Fatalf("키워드 순서 = %v, want [가격, 푸드트럭] (rank 순)", kws)
	}
}

func TestLoadClauseKeywords_ClauseGroupSearch(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	// q=친절 → clause '직원 친절' 또는 keyword '친절' 매칭 → 절 1개.
	_, total, items, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "친절", "clause", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("q=친절 clause group total=%d items=%d, want 1/1", total, len(items))
	}
	if fmt.Sprint(items[0]["clause"]) != "직원 친절" {
		t.Fatalf("q=친절 clause = %v, want 직원 친절", items[0]["clause"])
	}
}

// silverone 2026-06-19 — SNS 리포스트로 같은 절 텍스트가 여러 doc에 반복 → 절 표는
// 절 텍스트로 dedup. 같은 clause 2개 clause_id면 1행 + occurrence_count=2.
func TestLoadClauseKeywords_ClauseGroupDedup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ck.jsonl")
	lines := []string{
		// 동일 절 "맛 좋음"이 서로 다른 doc/clause_id로 2번 (리포스트).
		`{"doc_id":"d1","clause_id":"d1__1","clause":"맛 좋음","aspect":"food","sentiment":"positive","keyword":"맛","keyword_rank_in_clause":1,"source":"kiwi","extractor_version":"v"}`,
		`{"doc_id":"d2","clause_id":"d2__1","clause":"맛 좋음","aspect":"food","sentiment":"positive","keyword":"맛","keyword_rank_in_clause":1,"source":"kiwi","extractor_version":"v"}`,
		`{"doc_id":"d3","clause_id":"d3__1","clause":"공연 최고","aspect":"program","sentiment":"positive","keyword":"공연","keyword_rank_in_clause":1,"source":"kiwi","extractor_version":"v"}`,
	}
	if err := os.WriteFile(path, []byte(joinLines(lines)), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	_, total, items, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "", "clause", nil)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// clause_id는 3개지만 고유 절 텍스트는 2개 → dedup으로 2행.
	if total != 2 || len(items) != 2 {
		t.Fatalf("dedup total=%d items=%d, want 2/2 (고유 절)", total, len(items))
	}
	// "맛 좋음"의 occurrence_count = 2.
	var found bool
	for _, it := range items {
		if fmt.Sprint(it["clause"]) == "맛 좋음" {
			found = true
			if fmt.Sprint(it["occurrence_count"]) != "2" {
				t.Fatalf("'맛 좋음' occurrence_count=%v, want 2", it["occurrence_count"])
			}
		}
	}
	if !found {
		t.Fatal("'맛 좋음' 행이 없음")
	}
}

func TestBuildKeywordFilter(t *testing.T) {
	if got := buildKeywordFilter("", "", ""); got != "" {
		t.Fatalf("empty filter = %q, want empty", got)
	}
	if got := buildKeywordFilter("food", "", ""); got != "WHERE aspect = 'food'" {
		t.Fatalf("aspect filter = %q", got)
	}
	got := buildKeywordFilter("food", "negative", "가격")
	for _, want := range []string{"aspect = 'food'", "sentiment = 'negative'", "ILIKE"} {
		if !strings.Contains(got, want) {
			t.Fatalf("filter %q missing %q", got, want)
		}
	}
}

// silverone 2026-06-25 — 키워드 정제 사전 overlay 잠금. 사전을 source 서브쿼리로
// 감싸 block 제외/synonym 병합이 재집계까지 자동 반영되는지 검증.
func keywordItem(items []map[string]any, kw string) map[string]any {
	for _, it := range items {
		if fmt.Sprint(it["keyword"]) == kw {
			return it
		}
	}
	return nil
}

func TestLoadClauseKeywords_DictionaryBlock(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	rules := []domain.KeywordDictionaryRule{
		{RuleType: "block", SourceTerm: "가격", Active: true},
	}
	summary, total, items, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "", "", rules)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// 가격(2행) 제외 → total_keyword_count 6→4, distinct 5→4.
	if summary["total_keyword_count"] != 4 {
		t.Errorf("total_keyword_count=%v, want 4", summary["total_keyword_count"])
	}
	if summary["unique_keyword_count"] != 4 {
		t.Errorf("unique_keyword_count=%v, want 4", summary["unique_keyword_count"])
	}
	if total != 4 {
		t.Errorf("distinct keyword total=%d, want 4", total)
	}
	if keywordItem(items, "가격") != nil {
		t.Errorf("blocked keyword '가격' still present in items")
	}
}

func TestLoadClauseKeywords_DictionarySynonymMerge(t *testing.T) {
	path := setupClauseKeywordsFixture(t)
	// 푸드트럭 → 맛 병합. 맛(1) + 푸드트럭(1) = 2.
	rules := []domain.KeywordDictionaryRule{
		{RuleType: "synonym", SourceTerm: "푸드트럭", TargetTerm: "맛", Active: true},
	}
	summary, total, items, err := loadClauseKeywordsArtifact(path, 100, 0, "", "", "", "", rules)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	// 병합이라 총 언급(행) 수는 6 유지, distinct는 5→4.
	if summary["total_keyword_count"] != 6 {
		t.Errorf("total_keyword_count=%v, want 6 (merge keeps rows)", summary["total_keyword_count"])
	}
	if summary["unique_keyword_count"] != 4 {
		t.Errorf("unique_keyword_count=%v, want 4 after merge", summary["unique_keyword_count"])
	}
	if total != 4 {
		t.Errorf("distinct keyword total=%d, want 4", total)
	}
	if keywordItem(items, "푸드트럭") != nil {
		t.Errorf("merged source '푸드트럭' should not appear as its own keyword")
	}
	merged := keywordItem(items, "맛")
	if merged == nil {
		t.Fatalf("merged target '맛' missing")
	}
	if got := fmt.Sprint(merged["count"]); got != "2" {
		t.Errorf("merged '맛' count=%s, want 2 (맛+푸드트럭)", got)
	}
}

func TestValidateKeywordRuleAgainst(t *testing.T) {
	active := []domain.KeywordDictionaryRule{
		{RuleType: "block", SourceTerm: "광고", Active: true},
		{RuleType: "synonym", SourceTerm: "수제맥주", TargetTerm: "맥주", Active: true},
	}
	cases := []struct {
		name    string
		rt, src, tgt string
		wantErr bool
	}{
		{"synonym target in blocklist", "synonym", "비어", "광고", true},
		{"synonym target is another source (chain)", "synonym", "크래프트", "수제맥주", true},
		{"synonym source is a canonical target (re-merge)", "synonym", "맥주", "음료", true},
		{"valid new synonym", "synonym", "크래프트맥주", "맥주", false},
		{"valid new block", "block", "쓰레기", "", false},
	}
	for _, c := range cases {
		err := validateKeywordRuleAgainst(c.rt, c.src, c.tgt, active)
		if (err != nil) != c.wantErr {
			t.Errorf("%s: err=%v wantErr=%v", c.name, err, c.wantErr)
		}
	}
}
