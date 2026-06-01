package service

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// silverone 2026-06-01 — doc_genuineness / clause_label 다운로드 컬럼 확장
// (exportDocGenuinenessEnrichedCSV / exportClauseLabelEnrichedCSV) 잠금 테스트.
//
// 검증 포인트:
//   - cleaned.parquet 있으면 cleaned_text/raw_text/created_at/source_row_index 포함
//   - clause_label은 추가로 doc_genuineness.jsonl에서 genuineness LEFT JOIN
//   - clean / dg artifact 없거나 join miss인 row도 export 실패하지 않고 빈 값으로 유지
//   - clean 없으면 fallback (해당 컬럼 빈 값)
//   - CSV multiline / 쌍따옴표 escape (DuckDB COPY가 RFC4180 자동 처리)
//   - 컬럼 순서 고정

func TestExportDocGenuinenessEnrichedCSVWithCleanRef(t *testing.T) {
	dir := t.TempDir()
	jsonl := writeDocGenuinenessJSONL(t, dir, []dgRecord{
		{docID: "v:row:0", genuineness: "genuine_review", reason: "valid review text"},
		{docID: "v:row:1", genuineness: "non_review", reason: "ad\nwith newline and \"quotes\""},
	})
	parquet := writeCleanParquet(t, dir, []cleanRow{
		{rowID: "v:row:0", cleaned: "cleaned 0", raw: "raw 0", createdAt: "2026-01-01T00:00:00Z", srcIdx: 0},
		{rowID: "v:row:1", cleaned: "cleaned 1\nwith newline", raw: "raw 1", createdAt: "2026-01-02T00:00:00Z", srcIdx: 1},
	})

	csvPath, err := exportDocGenuinenessEnrichedCSV(jsonl, parquet)
	if err != nil {
		t.Fatalf("exportDocGenuinenessEnrichedCSV: %v", err)
	}
	defer os.Remove(csvPath)

	header, rows := readCSV(t, csvPath)
	wantHeader := []string{
		"doc_id", "genuineness", "reason", "prompt_version", "source",
		"cleaned_text", "raw_text", "created_at", "source_row_index",
	}
	assertHeader(t, header, wantHeader)
	if len(rows) != 2 {
		t.Fatalf("rows: want 2, got %d", len(rows))
	}
	// source_row_index ORDER BY로 첫 row가 0번이어야 함.
	if rows[0][0] != "v:row:0" || rows[0][5] != "cleaned 0" || rows[0][6] != "raw 0" {
		t.Errorf("row[0] mismatch: %v", rows[0])
	}
	// 두 번째 row는 reason과 cleaned_text 모두 multiline + 쌍따옴표 escape 통과.
	if rows[1][0] != "v:row:1" {
		t.Errorf("row[1].doc_id: %q", rows[1][0])
	}
	if !strings.Contains(rows[1][2], "newline") || !strings.Contains(rows[1][2], `"quotes"`) {
		t.Errorf("row[1].reason multiline/quote escape failed: %q", rows[1][2])
	}
	if !strings.Contains(rows[1][5], "newline") {
		t.Errorf("row[1].cleaned_text multiline escape failed: %q", rows[1][5])
	}
}

func TestExportDocGenuinenessEnrichedCSVWithoutCleanRefHasEmptyColumns(t *testing.T) {
	dir := t.TempDir()
	jsonl := writeDocGenuinenessJSONL(t, dir, []dgRecord{
		{docID: "v:row:0", genuineness: "mixed", reason: "ambiguous"},
	})

	csvPath, err := exportDocGenuinenessEnrichedCSV(jsonl, "")
	if err != nil {
		t.Fatalf("exportDocGenuinenessEnrichedCSV: %v", err)
	}
	defer os.Remove(csvPath)

	header, rows := readCSV(t, csvPath)
	wantHeader := []string{
		"doc_id", "genuineness", "reason", "prompt_version", "source",
		"cleaned_text", "raw_text", "created_at", "source_row_index",
	}
	assertHeader(t, header, wantHeader)
	if len(rows) != 1 {
		t.Fatalf("rows: want 1, got %d", len(rows))
	}
	// clean 4 컬럼은 모두 빈 문자열 (DuckDB COPY가 NULL → '')
	for i := 5; i < 9; i++ {
		if rows[0][i] != "" {
			t.Errorf("col[%d]: want empty (NULL), got %q", i, rows[0][i])
		}
	}
}

func TestExportDocGenuinenessEnrichedCSVJoinMissKeepsRow(t *testing.T) {
	dir := t.TempDir()
	jsonl := writeDocGenuinenessJSONL(t, dir, []dgRecord{
		{docID: "v:row:0", genuineness: "genuine_review", reason: "ok"},
		{docID: "v:row:99", genuineness: "non_review", reason: "missing in clean"},
	})
	parquet := writeCleanParquet(t, dir, []cleanRow{
		{rowID: "v:row:0", cleaned: "c0", raw: "r0", createdAt: "2026-01-01T00:00:00Z", srcIdx: 0},
	})

	csvPath, err := exportDocGenuinenessEnrichedCSV(jsonl, parquet)
	if err != nil {
		t.Fatalf("exportDocGenuinenessEnrichedCSV: %v", err)
	}
	defer os.Remove(csvPath)

	_, rows := readCSV(t, csvPath)
	if len(rows) != 2 {
		t.Fatalf("rows: want 2 (miss row preserved), got %d", len(rows))
	}
	// ORDER BY source_row_index NULLS LAST → v:row:0이 먼저, v:row:99가 뒤.
	if rows[0][0] != "v:row:0" {
		t.Errorf("rows[0].doc_id: want v:row:0, got %q", rows[0][0])
	}
	if rows[1][0] != "v:row:99" {
		t.Errorf("rows[1].doc_id: want v:row:99, got %q", rows[1][0])
	}
	// miss row는 clean 컬럼 4개 모두 빈.
	for i := 5; i < 9; i++ {
		if rows[1][i] != "" {
			t.Errorf("miss row col[%d]: want empty, got %q", i, rows[1][i])
		}
	}
}

func TestExportClauseLabelEnrichedCSVWithCleanAndDgJoin(t *testing.T) {
	dir := t.TempDir()
	clJSONL := writeClauseLabelJSONL(t, dir, []clRecord{
		{docID: "v:row:0", clause: "맛있다", aspect: "food", sentiment: "positive"},
		{docID: "v:row:0", clause: "분위기 좋다\n조용했음", aspect: "ambiance_scenery", sentiment: "positive"},
		{docID: "v:row:1", clause: "줄이 길었다", aspect: "operations", sentiment: "negative"},
	})
	parquet := writeCleanParquet(t, dir, []cleanRow{
		{rowID: "v:row:0", cleaned: "맛있고 분위기 좋다", raw: "원본0", createdAt: "2026-01-01T00:00:00Z", srcIdx: 0},
		{rowID: "v:row:1", cleaned: "줄이 길었다", raw: "원본1", createdAt: "2026-01-02T00:00:00Z", srcIdx: 1},
	})
	dgJSONL := writeDocGenuinenessJSONL(t, dir, []dgRecord{
		{docID: "v:row:0", genuineness: "genuine_review", reason: "ok"},
		{docID: "v:row:1", genuineness: "mixed", reason: "partial"},
	})

	csvPath, err := exportClauseLabelEnrichedCSV(clJSONL, parquet, dgJSONL)
	if err != nil {
		t.Fatalf("exportClauseLabelEnrichedCSV: %v", err)
	}
	defer os.Remove(csvPath)

	header, rows := readCSV(t, csvPath)
	wantHeader := []string{
		"doc_id", "clause_id", "clause_text", "aspect", "sentiment",
		"prompt_version", "source",
		"cleaned_text", "raw_text", "created_at", "source_row_index", "genuineness",
	}
	assertHeader(t, header, wantHeader)
	if len(rows) != 3 {
		t.Fatalf("rows: want 3, got %d", len(rows))
	}
	// clause_id: doc_id 0의 첫 절은 v:row:0-0, 두 번째는 v:row:0-1, doc 1의 첫
	// 절은 v:row:1-0. ORDER BY source_row_index, _rn 보장.
	wantClauseIDs := []string{"v:row:0-0", "v:row:0-1", "v:row:1-0"}
	for i, want := range wantClauseIDs {
		if rows[i][1] != want {
			t.Errorf("rows[%d].clause_id: want %q, got %q", i, want, rows[i][1])
		}
	}
	// multiline clause text escape.
	if !strings.Contains(rows[1][2], "조용했음") || !strings.Contains(rows[1][2], "분위기") {
		t.Errorf("rows[1].clause_text multiline escape failed: %q", rows[1][2])
	}
	// genuineness join 확인.
	if rows[0][11] != "genuine_review" || rows[2][11] != "mixed" {
		t.Errorf("genuineness join failed: rows[0]=%q rows[2]=%q", rows[0][11], rows[2][11])
	}
}

func TestExportClauseLabelEnrichedCSVWithoutAuxiliaryRefs(t *testing.T) {
	dir := t.TempDir()
	clJSONL := writeClauseLabelJSONL(t, dir, []clRecord{
		{docID: "v:row:0", clause: "ok", aspect: "food", sentiment: "neutral"},
	})

	csvPath, err := exportClauseLabelEnrichedCSV(clJSONL, "", "")
	if err != nil {
		t.Fatalf("exportClauseLabelEnrichedCSV: %v", err)
	}
	defer os.Remove(csvPath)

	header, rows := readCSV(t, csvPath)
	if got, want := len(header), 12; got != want {
		t.Errorf("header len: want %d, got %d (%v)", want, got, header)
	}
	if len(rows) != 1 {
		t.Fatalf("rows: want 1, got %d", len(rows))
	}
	// clean 4 + genuineness 1 = 5 컬럼이 모두 빈 (NULL).
	for i := 7; i < 12; i++ {
		if rows[0][i] != "" {
			t.Errorf("col[%d]: want empty (NULL), got %q", i, rows[0][i])
		}
	}
}

func TestExportClauseLabelEnrichedCSVDgJoinMissKeepsRow(t *testing.T) {
	dir := t.TempDir()
	clJSONL := writeClauseLabelJSONL(t, dir, []clRecord{
		{docID: "v:row:0", clause: "first", aspect: "food", sentiment: "positive"},
		{docID: "v:row:1", clause: "lost dg", aspect: "operations", sentiment: "neutral"},
	})
	parquet := writeCleanParquet(t, dir, []cleanRow{
		{rowID: "v:row:0", cleaned: "c0", raw: "r0", createdAt: "2026-01-01T00:00:00Z", srcIdx: 0},
		{rowID: "v:row:1", cleaned: "c1", raw: "r1", createdAt: "2026-01-02T00:00:00Z", srcIdx: 1},
	})
	dgJSONL := writeDocGenuinenessJSONL(t, dir, []dgRecord{
		{docID: "v:row:0", genuineness: "genuine_review", reason: "ok"},
		// v:row:1은 dg jsonl에 없음 → join miss
	})

	csvPath, err := exportClauseLabelEnrichedCSV(clJSONL, parquet, dgJSONL)
	if err != nil {
		t.Fatalf("exportClauseLabelEnrichedCSV must not fail on join miss: %v", err)
	}
	defer os.Remove(csvPath)

	_, rows := readCSV(t, csvPath)
	if len(rows) != 2 {
		t.Fatalf("rows: want 2 (dg miss row preserved), got %d", len(rows))
	}
	if rows[0][11] != "genuine_review" {
		t.Errorf("rows[0].genuineness: want genuine_review, got %q", rows[0][11])
	}
	if rows[1][11] != "" {
		t.Errorf("rows[1].genuineness (dg miss): want empty, got %q", rows[1][11])
	}
	// dg miss여도 cleaned_text는 살아 있어야.
	if rows[1][7] != "c1" {
		t.Errorf("rows[1].cleaned_text: want c1, got %q", rows[1][7])
	}
}

// ===== helpers =====

type dgRecord struct {
	docID       string
	genuineness string
	reason      string
}

type clRecord struct {
	docID     string
	clause    string
	aspect    string
	sentiment string
}

type cleanRow struct {
	rowID     string
	cleaned   string
	raw       string
	createdAt string
	srcIdx    int
}

func writeDocGenuinenessJSONL(t *testing.T, dir string, records []dgRecord) string {
	t.Helper()
	path := filepath.Join(dir, "dg-"+randomSuffix(t)+".jsonl")
	lines := make([]string, 0, len(records))
	for _, r := range records {
		// reason은 JSON string escape — json.Marshal 사용.
		lines = append(lines, fmt.Sprintf(
			`{"doc_id":%s,"genuineness":%s,"reason":%s,"source":"lloa","prompt_version":"dataset-doc-genuineness-v1"}`,
			jsonString(r.docID), jsonString(r.genuineness), jsonString(r.reason),
		))
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("write dg jsonl: %v", err)
	}
	return path
}

func writeClauseLabelJSONL(t *testing.T, dir string, records []clRecord) string {
	t.Helper()
	path := filepath.Join(dir, "cl-"+randomSuffix(t)+".jsonl")
	lines := make([]string, 0, len(records))
	for _, r := range records {
		lines = append(lines, fmt.Sprintf(
			`{"doc_id":%s,"clause":%s,"sentiment":%s,"aspect":%s,"source":"lloa","prompt_version":"dataset-clause-label-v3"}`,
			jsonString(r.docID), jsonString(r.clause), jsonString(r.sentiment), jsonString(r.aspect),
		))
	}
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
		t.Fatalf("write cl jsonl: %v", err)
	}
	return path
}

func writeCleanParquet(t *testing.T, dir string, rows []cleanRow) string {
	t.Helper()
	path := filepath.Join(dir, "clean-"+randomSuffix(t)+".parquet")
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		t.Fatalf("openTempDuckDB: %v", err)
	}
	defer cleanup()
	if len(rows) == 0 {
		t.Fatalf("writeCleanParquet requires at least 1 row")
	}
	selects := make([]string, 0, len(rows))
	for _, r := range rows {
		selects = append(selects, fmt.Sprintf(
			"SELECT %s AS row_id, %s AS cleaned_text, %s AS raw_text, %s AS created_at, %d AS source_row_index",
			sqlString(r.rowID), sqlString(r.cleaned), sqlString(r.raw), sqlString(r.createdAt), r.srcIdx,
		))
	}
	query := fmt.Sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)",
		strings.Join(selects, " UNION ALL "), path)
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("write parquet: %v\nquery: %s", err, query)
	}
	return path
}

func readCSV(t *testing.T, path string) ([]string, [][]string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open csv: %v", err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	reader.FieldsPerRecord = -1
	all, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}
	if len(all) == 0 {
		t.Fatalf("csv is empty")
	}
	return all[0], all[1:]
}

func assertHeader(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("header len: want %d, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("header[%d]: want %q, got %q", i, want[i], got[i])
		}
	}
}

func jsonString(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

var _suffixCounter int

func randomSuffix(t *testing.T) string {
	t.Helper()
	_suffixCounter++
	return fmt.Sprintf("%d-%d", os.Getpid(), _suffixCounter)
}

// silverone 2026-06-01 (다운로드 파일명 타임스탬프) — appendDownloadTimestamp /
// deriveDownloadFilename 휴리스틱 잠금. 같은 source ref라도 다운로드 시각이
// 다르면 파일명이 겹치지 않아야 한다.

func TestAppendDownloadTimestampInsertsBeforeExtension(t *testing.T) {
	got := appendDownloadTimestamp("doc_genuineness.csv")
	if !strings.HasPrefix(got, "doc_genuineness_") {
		t.Errorf("want prefix doc_genuineness_, got %q", got)
	}
	if !strings.HasSuffix(got, ".csv") {
		t.Errorf("want suffix .csv, got %q", got)
	}
	// `_YYYYMMDD_HHMMSS.csv` 형식 — 확장자 제외 14자 + underscore 1자.
	stem := strings.TrimSuffix(got, ".csv")
	idx := strings.LastIndex(stem, "_")
	if idx < 0 {
		t.Fatalf("no underscore in %q", stem)
	}
	tsPart := stem[idx-8:] // YYYYMMDD_HHMMSS (15자)
	if len(tsPart) != 15 || tsPart[8] != '_' {
		t.Errorf("timestamp shape mismatch: %q (got %q)", got, tsPart)
	}
}

func TestAppendDownloadTimestampWithoutExtension(t *testing.T) {
	got := appendDownloadTimestamp("cleaned")
	if !strings.HasPrefix(got, "cleaned_") {
		t.Errorf("want prefix cleaned_, got %q", got)
	}
	// 15자 timestamp만 남았어야.
	tail := strings.TrimPrefix(got, "cleaned_")
	if len(tail) != 15 || tail[8] != '_' {
		t.Errorf("timestamp shape mismatch: %q", got)
	}
}

func TestAppendDownloadTimestampWithEmptyNameReturnsTimestampOnly(t *testing.T) {
	got := appendDownloadTimestamp("")
	if len(got) != 15 || got[8] != '_' {
		t.Errorf("empty name should yield raw timestamp, got %q", got)
	}
}

func TestDeriveDownloadFilenameAppendsTimestamp(t *testing.T) {
	cases := []struct {
		ref      string
		fallback string
		wantBase string
	}{
		{"/data/clean.parquet", "cleaned.csv", "clean_"},
		{"doc_genuineness.jsonl", "doc_genuineness.csv", "doc_genuineness_"},
		{"clause_label.jsonl", "clause_label.csv", "clause_label_"},
		{"", "fallback.csv", "fallback_"},
	}
	for _, c := range cases {
		got := deriveDownloadFilename(c.ref, c.fallback)
		if !strings.HasPrefix(got, c.wantBase) {
			t.Errorf("ref=%q: want prefix %q, got %q", c.ref, c.wantBase, got)
		}
		if !strings.HasSuffix(got, ".csv") {
			t.Errorf("ref=%q: want .csv suffix, got %q", c.ref, got)
		}
	}
}
