package service

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

// exportDocGenuinenessEnrichedCSV — doc_genuineness.jsonl을 cleaned.parquet과
// row_id 기준 LEFT JOIN해 본문/원본 추적 컬럼을 포함한 CSV로 변환한다.
// silverone 2026-06-01 (다운로드 컬럼 확장) — view에 이미 cleaned_text join을
// 추가한 것과 같은 패턴. cleanRef가 비어 있거나 file이 없거나 JOIN이 실패하면
// jsonl 단독 export (기존 최소 컬럼)으로 fallback한다.
//
// 출력 컬럼 순서 (CSV header 고정):
//
//	doc_id, genuineness, reason, prompt_version, source,
//	cleaned_text, raw_text, created_at, source_row_index
//
// jsonl에 model / taxonomy / subject_name 필드는 저장되지 않으므로 export에
// 포함하지 않는다 (request에서 "가능하면"으로 표시된 항목들).
func exportDocGenuinenessEnrichedCSV(jsonlRef string, cleanRef string) (string, error) {
	jsonlRef = strings.TrimSpace(jsonlRef)
	cleanRef = strings.TrimSpace(cleanRef)
	if jsonlRef == "" {
		return "", ErrInvalidArgument{Message: "doc_genuineness artifact ref is required"}
	}
	if _, err := os.Stat(jsonlRef); err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound{Resource: "doc_genuineness artifact"}
		}
		return "", err
	}
	if cleanRef != "" {
		if _, err := os.Stat(cleanRef); err != nil {
			cleanRef = "" // file 없으면 join 생략
		}
	}

	csvPath, db, cleanup, err := prepareExportDuckDB("doc_genuineness")
	if err != nil {
		return "", err
	}
	defer cleanup()

	jsonlSource := fmt.Sprintf("read_json('%s', format='newline_delimited')", escapeDuckDBLiteral(jsonlRef))
	if cleanRef == "" {
		return runExportCopy(db, csvPath, fmt.Sprintf(
			`SELECT dg.doc_id, dg.genuineness, dg.reason,
			        dg.prompt_version, dg.source,
			        NULL AS cleaned_text, NULL AS raw_text,
			        NULL AS created_at, NULL AS source_row_index
			 FROM %s AS dg
			 ORDER BY dg.doc_id`,
			jsonlSource,
		))
	}

	cleanSource := fmt.Sprintf("read_parquet('%s')", escapeDuckDBLiteral(cleanRef))
	enrichedQuery := fmt.Sprintf(
		`SELECT dg.doc_id, dg.genuineness, dg.reason,
		        dg.prompt_version, dg.source,
		        c.cleaned_text, c.raw_text, c.created_at, c.source_row_index
		 FROM %s AS dg
		 LEFT JOIN %s AS c ON dg.doc_id = c.row_id
		 ORDER BY c.source_row_index NULLS LAST, dg.doc_id`,
		jsonlSource, cleanSource,
	)
	if path, err := runExportCopy(db, csvPath, enrichedQuery); err == nil {
		return path, nil
	}
	// JOIN 실패 (예: 옛 cleaned.parquet에 row_id 없음) — jsonl 단독 fallback.
	_ = os.Remove(csvPath)
	csvPath, db2, cleanup2, err := prepareExportDuckDB("doc_genuineness")
	if err != nil {
		return "", err
	}
	defer cleanup2()
	_ = db2
	return runExportCopy(db2, csvPath, fmt.Sprintf(
		`SELECT dg.doc_id, dg.genuineness, dg.reason,
		        dg.prompt_version, dg.source,
		        NULL AS cleaned_text, NULL AS raw_text,
		        NULL AS created_at, NULL AS source_row_index
		 FROM %s AS dg
		 ORDER BY dg.doc_id`,
		jsonlSource,
	))
}

// exportClauseLabelEnrichedCSV — clause_label.jsonl을 cleaned.parquet +
// doc_genuineness.jsonl과 doc_id 기준 LEFT JOIN해 본문/추적/genuineness
// 컬럼을 포함한 CSV로 변환한다. silverone 2026-06-01 (다운로드 컬럼 확장).
// cleanRef / dgRef 어느 쪽이든 비어 있거나 file이 없으면 그 컬럼만 NULL로
// 두고 row는 유지한다. JOIN 자체가 실패하면 jsonl 단독 fallback.
//
// 출력 컬럼 순서 (CSV header 고정):
//
//	doc_id, clause_id, clause_text, aspect, sentiment,
//	prompt_version, source,
//	cleaned_text, raw_text, created_at, source_row_index, genuineness
//
// jsonl에 aspect_reason / sentiment_reason / taxonomy_* 필드는 저장되지
// 않으므로 export에 포함하지 않는다 (request에서 "가능하면"으로 표시된 항목).
func exportClauseLabelEnrichedCSV(jsonlRef string, cleanRef string, dgRef string) (string, error) {
	jsonlRef = strings.TrimSpace(jsonlRef)
	cleanRef = strings.TrimSpace(cleanRef)
	dgRef = strings.TrimSpace(dgRef)
	if jsonlRef == "" {
		return "", ErrInvalidArgument{Message: "clause_label artifact ref is required"}
	}
	if _, err := os.Stat(jsonlRef); err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound{Resource: "clause_label artifact"}
		}
		return "", err
	}
	if cleanRef != "" {
		if _, err := os.Stat(cleanRef); err != nil {
			cleanRef = ""
		}
	}
	if dgRef != "" {
		if _, err := os.Stat(dgRef); err != nil {
			dgRef = ""
		}
	}

	csvPath, db, cleanup, err := prepareExportDuckDB("clause_label")
	if err != nil {
		return "", err
	}
	defer cleanup()

	jsonlSource := fmt.Sprintf("read_json('%s', format='newline_delimited')", escapeDuckDBLiteral(jsonlRef))

	// clause_id는 GetClauseLabelView와 동일 패턴 — jsonl scan 순서를 따라
	// doc_id별 ROW_NUMBER 0-base index 부여.
	baseSelect := fmt.Sprintf(`WITH ordered AS (
	    SELECT *, ROW_NUMBER() OVER () AS _rn
	    FROM %s
	),
	clauses AS (
	    SELECT
	        doc_id,
	        doc_id || '-' || CAST(ROW_NUMBER() OVER (PARTITION BY doc_id ORDER BY _rn) - 1 AS VARCHAR) AS clause_id,
	        clause AS clause_text,
	        aspect, sentiment, prompt_version, source,
	        _rn
	    FROM ordered
	)`, jsonlSource)

	joinClauses := ""
	cleanColumns := "NULL AS cleaned_text, NULL AS raw_text, NULL AS created_at, NULL AS source_row_index"
	if cleanRef != "" {
		joinClauses += fmt.Sprintf(" LEFT JOIN read_parquet('%s') AS c ON cl.doc_id = c.row_id",
			escapeDuckDBLiteral(cleanRef))
		cleanColumns = "c.cleaned_text, c.raw_text, c.created_at, c.source_row_index"
	}
	dgColumn := "NULL AS genuineness"
	if dgRef != "" {
		joinClauses += fmt.Sprintf(" LEFT JOIN read_json('%s', format='newline_delimited') AS dg ON cl.doc_id = dg.doc_id",
			escapeDuckDBLiteral(dgRef))
		dgColumn = "dg.genuineness"
	}
	orderBy := "ORDER BY cl._rn"
	if cleanRef != "" {
		orderBy = "ORDER BY c.source_row_index NULLS LAST, cl._rn"
	}

	enrichedQuery := fmt.Sprintf(
		`%s
		 SELECT cl.doc_id, cl.clause_id, cl.clause_text, cl.aspect, cl.sentiment,
		        cl.prompt_version, cl.source,
		        %s, %s
		 FROM clauses AS cl%s
		 %s`,
		baseSelect, cleanColumns, dgColumn, joinClauses, orderBy,
	)
	if path, err := runExportCopy(db, csvPath, enrichedQuery); err == nil {
		return path, nil
	}
	// JOIN 실패 시 jsonl 단독 fallback.
	_ = os.Remove(csvPath)
	csvPath2, db2, cleanup2, err := prepareExportDuckDB("clause_label")
	if err != nil {
		return "", err
	}
	defer cleanup2()
	fallbackQuery := fmt.Sprintf(
		`%s
		 SELECT cl.doc_id, cl.clause_id, cl.clause_text, cl.aspect, cl.sentiment,
		        cl.prompt_version, cl.source,
		        NULL AS cleaned_text, NULL AS raw_text,
		        NULL AS created_at, NULL AS source_row_index,
		        NULL AS genuineness
		 FROM clauses AS cl
		 ORDER BY cl._rn`,
		baseSelect,
	)
	return runExportCopy(db2, csvPath2, fallbackQuery)
}

// prepareExportDuckDB — temp CSV 경로 + temp DuckDB handle + cleanup callback
// 생성. exportDocGenuinenessEnrichedCSV / exportClauseLabelEnrichedCSV 공유.
func prepareExportDuckDB(kind string) (string, *sql.DB, func(), error) {
	csvHandle, err := os.CreateTemp("", kind+"-export-*.csv")
	if err != nil {
		return "", nil, nil, err
	}
	csvPath := csvHandle.Name()
	if err := csvHandle.Close(); err != nil {
		return "", nil, nil, err
	}
	if err := os.Remove(csvPath); err != nil && !os.IsNotExist(err) {
		return "", nil, nil, err
	}

	tempHandle, err := os.CreateTemp("", kind+"-export-*.duckdb")
	if err != nil {
		return "", nil, nil, err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return "", nil, nil, err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return "", nil, nil, err
	}

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		_ = os.Remove(dbPath)
		return "", nil, nil, err
	}
	cleanup := func() {
		_ = db.Close()
		_ = os.Remove(dbPath)
	}
	return csvPath, db, cleanup, nil
}

// runExportCopy — DuckDB COPY ... TO ... (FORMAT CSV, HEADER) 실행.
// 실패 시 csvPath를 정리하고 에러 반환. 성공 시 csvPath 그대로 반환.
// CSV escaping(쌍따옴표/개행)은 DuckDB COPY가 RFC4180 호환으로 자동 처리.
func runExportCopy(db *sql.DB, csvPath string, selectQuery string) (string, error) {
	query := fmt.Sprintf(
		`COPY (%s) TO '%s' (FORMAT CSV, HEADER)`,
		selectQuery, escapeDuckDBLiteral(csvPath),
	)
	if _, err := db.Exec(query); err != nil {
		_ = os.Remove(csvPath)
		return "", err
	}
	return csvPath, nil
}

func exportCleanCSVFromParquet(cleanedRef string) (string, error) {
	cleanedRef = strings.TrimSpace(cleanedRef)
	if cleanedRef == "" {
		return "", ErrInvalidArgument{Message: "clean artifact ref is required"}
	}
	if _, err := os.Stat(cleanedRef); err != nil {
		if os.IsNotExist(err) {
			return "", ErrNotFound{Resource: "clean artifact"}
		}
		return "", err
	}

	csvHandle, err := os.CreateTemp("", "clean-export-*.csv")
	if err != nil {
		return "", err
	}
	csvPath := csvHandle.Name()
	if err := csvHandle.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(csvPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}

	tempHandle, err := os.CreateTemp("", "clean-export-*.duckdb")
	if err != nil {
		return "", err
	}
	dbPath := tempHandle.Name()
	if err := tempHandle.Close(); err != nil {
		return "", err
	}
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	defer os.Remove(dbPath)

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return "", err
	}
	defer db.Close()

	// silverone 2026-05-26 — 옛 cleaned.parquet은 source_row_index 컬럼이 없을 수
	// 있어 (4 col schema) 무조건 ORDER BY를 박으면 Binder Error로 500이 난다.
	// parquet schema를 먼저 보고 존재하는 컬럼만 골라 ORDER BY를 짠다. 둘 다 없는
	// edge case는 ORDER BY 생략 (parquet native order).
	orderClause, err := buildCleanExportOrderClause(db, cleanedRef)
	if err != nil {
		return "", err
	}
	query := fmt.Sprintf(
		`COPY (
			SELECT * FROM read_parquet('%s')
			%s
		) TO '%s' (FORMAT CSV, HEADER)`,
		escapeDuckDBLiteral(cleanedRef),
		orderClause,
		escapeDuckDBLiteral(csvPath),
	)
	if _, err := db.Exec(query); err != nil {
		_ = os.Remove(csvPath)
		return "", err
	}
	return csvPath, nil
}

// buildCleanExportOrderClause — cleaned.parquet schema에서 source_row_index /
// row_id 존재 여부를 보고 ORDER BY 절을 만든다. 결과는 빈 문자열이거나
// "ORDER BY ..." 형식. SQL injection은 column 이름이 hardcoded 화이트리스트라 없음.
func buildCleanExportOrderClause(db *sql.DB, cleanedRef string) (string, error) {
	rows, err := db.Query(fmt.Sprintf(
		`SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('%s'))`,
		escapeDuckDBLiteral(cleanedRef),
	))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	cols := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return "", err
		}
		cols[strings.TrimSpace(name)] = true
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	parts := make([]string, 0, 2)
	if cols["source_row_index"] {
		parts = append(parts, "source_row_index")
	}
	if cols["row_id"] {
		parts = append(parts, "row_id")
	}
	if len(parts) == 0 {
		return "", nil
	}
	return "ORDER BY " + strings.Join(parts, ", "), nil
}
