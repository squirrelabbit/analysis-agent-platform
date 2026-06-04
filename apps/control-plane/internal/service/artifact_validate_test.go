package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// validParquetBytes — magic-bytes 검증(PAR1 framing)을 통과하는 최소 parquet 바이트.
// 실제 row는 없지만 컨테이너 framing은 유효 — checkParquetFraming용 fixture.
func validParquetBytes() []byte {
	return []byte("PAR1\x00\x00\x00\x00PAR1")
}

func writeTemp(t *testing.T, name string, body []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, body, 0o644); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

func TestValidateArtifactReadable_Invalid(t *testing.T) {
	dirPath := t.TempDir()

	cases := []struct {
		name      string
		path      string
		format    artifactFormat
		wantCause string // err 메시지에 포함되어야 하는 원인 조각
	}{
		{"missing", filepath.Join(t.TempDir(), "nope.parquet"), artifactParquet, "missing on disk"},
		{"empty path", "", artifactParquet, "경로가 비어"},
		{"directory", dirPath, artifactParquet, "디렉터리"},
		{"zero byte parquet", writeTemp(t, "z.parquet", []byte{}), artifactParquet, "0 bytes"},
		{"zero byte jsonl", writeTemp(t, "z.jsonl", []byte{}), artifactJSONL, "0 bytes"},
		{"corrupt parquet (no PAR1)", writeTemp(t, "c.parquet", []byte("GARBAGE-not-parquet")), artifactParquet, "PAR1"},
		{"truncated parquet (<8B)", writeTemp(t, "t.parquet", []byte("PAR1")), artifactParquet, "8바이트 미만"},
		{"corrupt jsonl", writeTemp(t, "c.jsonl", []byte("this is not json\n")), artifactJSONL, "JSON"},
		{"blank-only jsonl", writeTemp(t, "b.jsonl", []byte("\n   \n")), artifactJSONL, "non-empty"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateArtifactReadable("clauses", c.path, c.format)
			if err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
			if _, ok := err.(ErrInvalidArgument); !ok {
				t.Fatalf("expected ErrInvalidArgument, got %T: %v", err, err)
			}
			if !strings.Contains(err.Error(), c.wantCause) {
				t.Fatalf("err %q missing cause %q", err.Error(), c.wantCause)
			}
		})
	}
}

func TestValidateArtifactReadable_Valid(t *testing.T) {
	cases := []struct {
		name   string
		path   string
		format artifactFormat
	}{
		{"valid parquet", writeTemp(t, "ok.parquet", validParquetBytes()), artifactParquet},
		{"valid jsonl", writeTemp(t, "ok.jsonl", []byte(`{"doc_id":"d1"}`+"\n")), artifactJSONL},
		{"jsonl with leading blank line", writeTemp(t, "lb.jsonl", []byte("\n"+`{"a":1}`+"\n")), artifactJSONL},
		{"valid csv", writeTemp(t, "ok.csv", []byte("a,b\n1,2\n")), artifactCSV},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := validateArtifactReadable("docs", c.path, c.format); err != nil {
				t.Fatalf("expected valid, got error: %v", err)
			}
		})
	}
}

// 실패 메시지에 label / path / format이 모두 들어있어 운영자가 바로 조치 가능해야 한다.
func TestValidateArtifactReadable_MessageHasLabelPathFormat(t *testing.T) {
	path := writeTemp(t, "broken.jsonl", []byte("garbage\n"))
	err := validateArtifactReadable("clauses", path, artifactJSONL)
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	for _, want := range []string{"clauses", path, "jsonl"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("err %q missing %q", msg, want)
		}
	}
}
