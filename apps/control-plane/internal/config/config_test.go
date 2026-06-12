package config

import "testing"

// 전처리 모델 선택 allowlist 파서 잠금 (2026-06-12).
func TestParseLLOAModelOptions(t *testing.T) {
	t.Run("라벨 포함 목록 + default는 LLOA_MODEL 일치 항목", func(t *testing.T) {
		options := parseLLOAModelOptions(
			"wisenut/wise-lloa-max-v1.2.1=LLOA Max 1.2.1, wisenut/wise-lloa-ultra-v1.1.0=LLOA Ultra 1.1.0",
			"wisenut/wise-lloa-ultra-v1.1.0", "",
		)
		if len(options) != 2 {
			t.Fatalf("expected 2 options, got %d: %+v", len(options), options)
		}
		if options[0].ModelID != "wisenut/wise-lloa-max-v1.2.1" || options[0].Label != "LLOA Max 1.2.1" || options[0].Default {
			t.Fatalf("unexpected first option: %+v", options[0])
		}
		if !options[1].Default {
			t.Fatalf("LLOA_MODEL 일치 항목이 default여야 함: %+v", options)
		}
	})

	t.Run("라벨 생략 시 id가 라벨", func(t *testing.T) {
		options := parseLLOAModelOptions("wisenut/wise-lloa-max-v1.2.1", "", "")
		if len(options) != 1 || options[0].Label != "wisenut/wise-lloa-max-v1.2.1" {
			t.Fatalf("unexpected options: %+v", options)
		}
		// LLOA_MODEL 미일치 시 첫 항목이 default.
		if !options[0].Default {
			t.Fatalf("첫 항목 default fallback 실패: %+v", options)
		}
	})

	t.Run("중복 id는 첫 항목만 유지", func(t *testing.T) {
		options := parseLLOAModelOptions("a=One,a=Two,b", "b", "")
		if len(options) != 2 || options[0].Label != "One" {
			t.Fatalf("unexpected options: %+v", options)
		}
	})

	t.Run("미설정 시 LLOA_MODEL 단일 항목 fallback", func(t *testing.T) {
		options := parseLLOAModelOptions("", "wisenut/wise-lloa-max-v1.2.1", "LLOA Max")
		if len(options) != 1 || options[0].ModelID != "wisenut/wise-lloa-max-v1.2.1" ||
			options[0].Label != "LLOA Max" || !options[0].Default {
			t.Fatalf("unexpected fallback options: %+v", options)
		}
	})

	t.Run("둘 다 없으면 빈 목록", func(t *testing.T) {
		if options := parseLLOAModelOptions("", "", ""); options != nil {
			t.Fatalf("expected nil, got %+v", options)
		}
	})
}
