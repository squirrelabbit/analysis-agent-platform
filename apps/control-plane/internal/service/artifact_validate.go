package service

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// artifact 검증 — silverone 2026-06-04.
// analyze/download view 진입 전에 artifact 파일이 "실제로 읽을 수 있는 상태"인지
// lightweight하게 확인한다. worker/DuckDB가 나중에 읽다 실패하기 전에, 운영자가 바로
// 조치할 수 있는 명확한 에러(label/path/format + 원인 + 조치)를 돌려주는 게 목적.
// checksum/row_count/schema 메타 저장 등 무거운 lifecycle은 범위 밖.

type artifactFormat string

const (
	artifactParquet artifactFormat = "parquet"
	artifactJSONL   artifactFormat = "jsonl"
	artifactCSV     artifactFormat = "csv"
)

// jsonl/csv 한 줄이 길 수 있어(clause_label 등) scanner 버퍼 상한을 키운다.
const artifactScanMaxLine = 16 * 1024 * 1024

// validateArtifactReadable — 공통 검증(존재/regular file/size>0) + format별 lightweight read.
// 실패 시 ErrInvalidArgument(label/path/format/원인/조치 포함). 통과 시 nil.
func validateArtifactReadable(label, path string, format artifactFormat) error {
	if strings.TrimSpace(path) == "" {
		return artifactInvalid(label, path, format, "경로가 비어 있음")
	}
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return artifactInvalid(label, path, format, "디스크에 없음 (missing on disk)")
		}
		return fmt.Errorf("artifact %q stat 실패 (%s): %w", label, path, err)
	}
	if info.IsDir() {
		return artifactInvalid(label, path, format, "파일이 아니라 디렉터리임")
	}
	if !info.Mode().IsRegular() {
		return artifactInvalid(label, path, format, "정규 파일이 아님 (regular file 아님)")
	}
	if info.Size() == 0 {
		return artifactInvalid(label, path, format, "비어 있음 (0 bytes)")
	}

	var cause string
	switch format {
	case artifactParquet:
		cause = checkParquetFraming(path)
	case artifactJSONL:
		cause = checkJSONLFirstLine(path)
	case artifactCSV:
		cause = checkCSVFirstLine(path)
	}
	if cause != "" {
		return artifactInvalid(label, path, format, cause)
	}
	return nil
}

func artifactInvalid(label, path string, format artifactFormat, cause string) error {
	return ErrInvalidArgument{Message: fmt.Sprintf(
		"artifact %q (%s) [%s] 검증 실패: %s — 해당 dataset version의 빌드를 다시 실행하거나 업로드를 확인하세요.",
		label, path, format, cause,
	)}
}

// checkParquetFraming — parquet 컨테이너 framing(PAR1 magic, 앞뒤 4바이트)을 확인한다.
// 풀 schema scan 대신 lightweight 검증 — zero-byte / truncated write / 잘못된 포맷 /
// 깨진 컨테이너를 잡는다(분석 hot path에 DuckDB 연결을 걸지 않는다). 빈 문자열이면 정상.
//
// ⚠️ 한계: PAR1 framing만 보므로 *내부 row group / schema 손상*까지는 보장하지 않는다.
// framing이 멀쩡한데 내부가 깨진 경우의 최종 read 오류는 Python/DuckDB 실행 단계에서
// 여전히 발생할 수 있다. 이 검증은 "빠르고 흔한 손상(잘림/오포맷/빈 파일)을 조기 차단"이 목적.
func checkParquetFraming(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Sprintf("열 수 없음: %v", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Sprintf("stat 실패: %v", err)
	}
	if info.Size() < 8 {
		return "parquet 형식 아님 (8바이트 미만 — 잘렸거나 손상)"
	}
	head := make([]byte, 4)
	if _, err := f.ReadAt(head, 0); err != nil {
		return fmt.Sprintf("머리 4바이트 read 실패: %v", err)
	}
	foot := make([]byte, 4)
	if _, err := f.ReadAt(foot, info.Size()-4); err != nil {
		return fmt.Sprintf("꼬리 4바이트 read 실패: %v", err)
	}
	if string(head) != "PAR1" || string(foot) != "PAR1" {
		return "parquet PAR1 magic 없음 (잘렸거나 parquet이 아님 — 손상 의심)"
	}
	return ""
}

// checkJSONLFirstLine — 첫 non-empty line이 JSON으로 parse 가능한지 확인. 빈 문자열이면 정상.
func checkJSONLFirstLine(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Sprintf("열 수 없음: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), artifactScanMaxLine)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		if !json.Valid(line) {
			return "첫 줄이 유효한 JSON이 아님 (jsonl 손상)"
		}
		return ""
	}
	if err := sc.Err(); err != nil {
		return fmt.Sprintf("read 실패: %v", err)
	}
	return "non-empty 줄이 없음 (빈 jsonl)"
}

// checkCSVFirstLine — 첫 non-empty line read 가능 여부만 확인(관대). 빈 문자열이면 정상.
func checkCSVFirstLine(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Sprintf("열 수 없음: %v", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), artifactScanMaxLine)
	for sc.Scan() {
		if len(bytes.TrimSpace(sc.Bytes())) > 0 {
			return ""
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Sprintf("read 실패: %v", err)
	}
	return "non-empty 줄이 없음 (빈 csv)"
}
