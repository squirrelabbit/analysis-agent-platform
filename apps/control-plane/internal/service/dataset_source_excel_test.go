package service

import (
	"path/filepath"
	"testing"

	"github.com/xuri/excelize/v2"
)

// 엑셀(.xlsx/.xlsm) 업로드: source 포맷 인식 + 프리뷰(컬럼/행수/샘플) 실제 읽기.
// clean의 text_columns 선택이 source 컬럼 목록에 의존하므로 컬럼이 채워져야 한다.
// Python worker(runtime.common._read_excel_rows)와 정합: 첫 행 헤더, 빈행 skip, 문자열 값.
func TestInferDatasetSourceFormatExcel(t *testing.T) {
	cases := map[string]string{
		"projects/p/d/v/source/data.xlsx": "xlsx",
		"data.XLSM":                       "xlsx",
		"data.csv":                        "csv",
		"data.bin":                        "",
	}
	for path, want := range cases {
		if got := inferDatasetSourceFormat(path); got != want {
			t.Errorf("inferDatasetSourceFormat(%q) = %q, want %q", path, got, want)
		}
	}
}

func writeTestXlsx(t *testing.T, dir string) string {
	t.Helper()
	f := excelize.NewFile()
	sheet := f.GetSheetName(0)
	_ = f.SetSheetRow(sheet, "A1", &[]any{"제목", "좋아요수", "본문"})
	_ = f.SetSheetRow(sheet, "A2", &[]any{"축제 후기", 123, "맥주가 맛있었어요"})
	_ = f.SetSheetRow(sheet, "A3", &[]any{"둘째날", 0, "사람이 많았어요"})
	path := filepath.Join(dir, "sample.xlsx")
	if err := f.SaveAs(path); err != nil {
		t.Fatalf("save xlsx: %v", err)
	}
	return path
}

func TestLoadDatasetSourceSummaryExcelReadsColumns(t *testing.T) {
	path := writeTestXlsx(t, t.TempDir())
	summary := loadDatasetSourceSummary(path, 5)

	if summary.Format != "xlsx" {
		t.Errorf("Format = %q, want xlsx", summary.Format)
	}
	if !summary.Available || summary.Status != "ready" {
		t.Errorf("Available=%v Status=%q, want available/ready", summary.Available, summary.Status)
	}
	if summary.ErrorMessage != "" {
		t.Errorf("ErrorMessage = %q, want empty", summary.ErrorMessage)
	}
	gotCols := make([]string, 0, len(summary.Columns))
	for _, c := range summary.Columns {
		gotCols = append(gotCols, c.Name)
	}
	if len(gotCols) != 3 || gotCols[0] != "제목" || gotCols[2] != "본문" {
		t.Errorf("Columns = %v, want [제목 좋아요수 본문]", gotCols)
	}
	if summary.RowCount == nil || *summary.RowCount != 2 {
		t.Errorf("RowCount = %v, want 2", summary.RowCount)
	}
	if len(summary.SampleRows) != 2 || summary.SampleRows[0]["좋아요수"] != "123" {
		t.Errorf("SampleRows = %v, want 2 rows with 좋아요수=123", summary.SampleRows)
	}
}

func TestLoadDatasetSourceSummaryExcelMissingFile(t *testing.T) {
	summary := loadDatasetSourceSummary(filepath.Join(t.TempDir(), "nope.xlsx"), 5)
	if summary.Status != "missing" {
		t.Errorf("Status = %q, want missing", summary.Status)
	}
}
