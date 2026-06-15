package service

import (
	"os"
	"path/filepath"
	"testing"
)

// 전처리 모델 선택 catalog 로더 잠금 (2026-06-12, config/lloa_models.json).
func TestLoadLLOAModelOptions(t *testing.T) {
	write := func(t *testing.T, body string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "lloa_models.json")
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("write fixture: %v", err)
		}
		return path
	}

	t.Run("default 일치 항목에 Default=true", func(t *testing.T) {
		path := write(t, `{
			"default": "wisenut/wise-lloa-ultra-v1.1.0",
			"models": [
				{"model_id": "wisenut/wise-lloa-max-v1.2.1", "label": "LLOA Max 1.2.1"},
				{"model_id": "wisenut/wise-lloa-ultra-v1.1.0", "label": "LLOA Ultra 1.1.0"}
			]
		}`)
		options, err := loadLLOAModelOptions(path)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if len(options) != 2 {
			t.Fatalf("expected 2, got %d: %+v", len(options), options)
		}
		if options[0].Default {
			t.Fatalf("first should not be default: %+v", options[0])
		}
		if !options[1].Default || options[1].ModelID != "wisenut/wise-lloa-ultra-v1.1.0" {
			t.Fatalf("ultra should be default: %+v", options)
		}
	})

	t.Run("default 미일치/누락이면 첫 항목 default", func(t *testing.T) {
		path := write(t, `{"models": [{"model_id": "a", "label": "A"}, {"model_id": "b", "label": "B"}]}`)
		options, err := loadLLOAModelOptions(path)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if !options[0].Default || options[1].Default {
			t.Fatalf("first item should be default fallback: %+v", options)
		}
	})

	t.Run("label 생략 시 model_id가 label, 중복 id 제거", func(t *testing.T) {
		path := write(t, `{"models": [{"model_id": "a"}, {"model_id": "a", "label": "dup"}, {"model_id": " "}]}`)
		options, err := loadLLOAModelOptions(path)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if len(options) != 1 || options[0].Label != "a" {
			t.Fatalf("unexpected options: %+v", options)
		}
	})

	t.Run("파일 부재는 빈 목록(정상)", func(t *testing.T) {
		options, err := loadLLOAModelOptions(filepath.Join(t.TempDir(), "missing.json"))
		if err != nil {
			t.Fatalf("missing file should not error: %v", err)
		}
		if options != nil {
			t.Fatalf("expected nil, got %+v", options)
		}
	})

	t.Run("빈 경로는 빈 목록", func(t *testing.T) {
		if options, err := loadLLOAModelOptions("  "); err != nil || options != nil {
			t.Fatalf("blank path: options=%+v err=%v", options, err)
		}
	})

	t.Run("손상된 JSON은 error", func(t *testing.T) {
		path := write(t, `{not json`)
		if _, err := loadLLOAModelOptions(path); err == nil {
			t.Fatalf("expected error for malformed json")
		}
	})

	t.Run("SetLLOAModelsPath로 service에 주입", func(t *testing.T) {
		path := write(t, `{"default": "a", "models": [{"model_id": "a", "label": "A"}]}`)
		service := NewDatasetService(nil, "", t.TempDir(), t.TempDir())
		if err := service.SetLLOAModelsPath(path); err != nil {
			t.Fatalf("SetLLOAModelsPath: %v", err)
		}
		got := service.LLOAModelOptions()
		if len(got) != 1 || got[0].ModelID != "a" || !got[0].Default {
			t.Fatalf("unexpected injected options: %+v", got)
		}
	})
}
