package domain

import "testing"

func TestResolveDatasetSourceUsesPreparedBeforeClean(t *testing.T) {
	prepareURI := "prepared.parquet"
	cleanedRef := "cleaned.parquet"
	version := DatasetVersion{
		StorageURI:    "raw.csv",
		PrepareStatus: "ready",
		PrepareURI:    &prepareURI,
		CleanedRef:    &cleanedRef,
		Metadata: map[string]any{
			"clean_status":         "ready",
			"cleaned_ref":          cleanedRef,
			"cleaned_text_column":  "cleaned_text",
			"prepared_text_column": "normalized_text",
			"raw_text_columns":     []string{"제목", "본문"},
			"raw_text_column":      "제목 + 본문",
			"text_columns":         []string{"제목", "본문"},
			"text_column":          "제목 + 본문",
		},
	}

	got := ResolveDatasetSource(version)
	if got.Stage != DatasetSourceStagePrepared {
		t.Fatalf("expected prepared stage, got %s", got.Stage)
	}
	if got.DatasetName != prepareURI {
		t.Fatalf("expected prepared ref, got %s", got.DatasetName)
	}
	if got.TextColumn != "normalized_text" {
		t.Fatalf("expected normalized_text, got %s", got.TextColumn)
	}
}

func TestResolveDatasetSourceUsesCleanBeforeRaw(t *testing.T) {
	version := DatasetVersion{
		StorageURI:    "raw.csv",
		PrepareStatus: "not_requested",
		Metadata: map[string]any{
			"clean_status":        "ready",
			"cleaned_ref":         "cleaned.parquet",
			"cleaned_text_column": "cleaned_text",
			"raw_text_columns":    []string{"제목", "본문"},
			"raw_text_column":     "제목 + 본문",
		},
	}

	got := ResolveDatasetSource(version)
	if got.Stage != DatasetSourceStageClean {
		t.Fatalf("expected clean stage, got %s", got.Stage)
	}
	if got.DatasetName != "cleaned.parquet" {
		t.Fatalf("expected cleaned ref, got %s", got.DatasetName)
	}
	if got.TextColumn != "cleaned_text" {
		t.Fatalf("expected cleaned_text, got %s", got.TextColumn)
	}
	if !DatasetSourceIsRawTextColumn(version, "제목") {
		t.Fatalf("expected raw column to be recognized")
	}
	if !DatasetSourceIsRawTextColumn(version, "text") {
		t.Fatalf("expected default text placeholder to be recognized")
	}
}

func TestResolveRawDatasetSourceUsesTextColumns(t *testing.T) {
	version := DatasetVersion{
		StorageURI: "raw.csv",
		Metadata: map[string]any{
			"text_columns": []any{"제목", "본문"},
		},
	}

	got := ResolveRawDatasetSource(version)
	if got.Stage != DatasetSourceStageRaw {
		t.Fatalf("expected raw stage, got %s", got.Stage)
	}
	if got.DatasetName != "raw.csv" {
		t.Fatalf("expected raw storage uri, got %s", got.DatasetName)
	}
	if got.TextColumn != "text" {
		t.Fatalf("expected text fallback for multi-column raw source, got %s", got.TextColumn)
	}
	if len(got.TextColumns) != 2 || got.TextColumns[0] != "제목" || got.TextColumns[1] != "본문" {
		t.Fatalf("unexpected text columns: %+v", got.TextColumns)
	}
}
