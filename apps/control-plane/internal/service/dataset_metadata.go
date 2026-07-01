package service

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"analysis-support-platform/control-plane/internal/domain"
)

func normalizeDatasetProfile(profile *domain.DatasetProfile) *domain.DatasetProfile {
	if profile == nil {
		return nil
	}
	normalized := &domain.DatasetProfile{
		ProfileID:              strings.TrimSpace(profile.ProfileID),
		PreparePromptVersion:   trimStringPointer(profile.PreparePromptVersion),
		SentimentPromptVersion: trimStringPointer(profile.SentimentPromptVersion),
		RegexRuleNames:         normalizeStringList(profile.RegexRuleNames),
		GarbageRuleNames:       normalizeStringList(profile.GarbageRuleNames),
		EmbeddingModel:         trimStringPointer(profile.EmbeddingModel),
	}
	if normalized.ProfileID == "" &&
		normalized.PreparePromptVersion == nil &&
		normalized.SentimentPromptVersion == nil &&
		len(normalized.RegexRuleNames) == 0 &&
		len(normalized.GarbageRuleNames) == 0 &&
		normalized.EmbeddingModel == nil {
		return nil
	}
	return normalized
}

func cloneDatasetProfile(profile *domain.DatasetProfile) *domain.DatasetProfile {
	if profile == nil {
		return nil
	}
	cloned := &domain.DatasetProfile{
		ProfileID:        profile.ProfileID,
		RegexRuleNames:   append([]string(nil), profile.RegexRuleNames...),
		GarbageRuleNames: append([]string(nil), profile.GarbageRuleNames...),
	}
	if profile.PreparePromptVersion != nil {
		value := strings.TrimSpace(*profile.PreparePromptVersion)
		cloned.PreparePromptVersion = &value
	}
	if profile.SentimentPromptVersion != nil {
		value := strings.TrimSpace(*profile.SentimentPromptVersion)
		cloned.SentimentPromptVersion = &value
	}
	if profile.EmbeddingModel != nil {
		value := strings.TrimSpace(*profile.EmbeddingModel)
		cloned.EmbeddingModel = &value
	}
	return cloned
}

func normalizeStringList(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

type datasetBuildTextSelection struct {
	TextColumn string
	Columns    []string
	Joiner     string
}

const defaultDatasetBuildTextJoiner = "\n\n"

func resolveDatasetBuildTextSelection(
	metadata map[string]any,
	inputColumns []string,
) datasetBuildTextSelection {
	columns := normalizeStringList(inputColumns)
	if len(columns) == 0 {
		columns = metadataStringList(metadata, "raw_text_columns")
	}
	if len(columns) == 0 {
		columns = metadataStringList(metadata, "text_columns")
	}
	return datasetBuildTextSelection{
		TextColumn: datasetBuildTextColumnLabel(columns),
		Columns:    append([]string(nil), columns...),
		Joiner:     defaultDatasetBuildTextJoiner,
	}
}

// 2026-05-21 — clean preprocess boolean option(remove_english/remove_numbers
// /remove_special/remove_monosyllables) 4종 제거. 한글 SNS 후기 분석 흐름에서
// 영문/숫자/공백/모노 음절은 모두 의미 신호라 거친 제거는 해가 됐다.
// dataset domain별 미세조정이 필요해지면 `regex_rule_names` (config/regex_rules
// JSON) + `noise_patterns` (config/noise_patterns JSON)로 명시 룰을 추가한다.

func datasetBuildTextColumnLabel(columns []string) string {
	normalized := normalizeStringList(columns)
	if len(normalized) == 0 {
		return ""
	}
	if len(normalized) == 1 {
		return normalized[0]
	}
	return strings.Join(normalized, " + ")
}

func metadataTime(metadata map[string]any, key string) (time.Time, bool) {
	if metadata == nil {
		return time.Time{}, false
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return time.Time{}, false
	}
	switch typed := value.(type) {
	case time.Time:
		return typed, true
	case string:
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(typed))
		return parsed, err == nil
	default:
		return time.Time{}, false
	}
}

func metadataStringList(metadata map[string]any, key string) []string {
	if metadata == nil {
		return nil
	}
	value, ok := metadata[key]
	if !ok {
		return nil
	}
	return anyStringList(value)
}

func anyStringList(value any) []string {
	switch typed := value.(type) {
	case []string:
		return normalizeStringList(typed)
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			values = append(values, anyStringValue(item))
		}
		return normalizeStringList(values)
	case string:
		return normalizeStringList([]string{typed})
	default:
		return nil
	}
}

func trimStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

func shouldActivateDatasetVersionOnCreate(value *bool) bool {
	if value == nil {
		return true
	}
	return *value
}

func markDatasetVersionActive(version *domain.DatasetVersion, dataset domain.Dataset) {
	if version == nil {
		return
	}
	version.IsActive = dataset.ActiveDatasetVersionID != nil && *dataset.ActiveDatasetVersionID == version.DatasetVersionID
}

func (s *DatasetService) saveDatasetActiveVersion(dataset domain.Dataset, datasetVersionID *string) (domain.Dataset, error) {
	dataset.ActiveDatasetVersionID = trimStringPointer(datasetVersionID)
	now := time.Now().UTC()
	dataset.ActiveVersionUpdatedAt = &now
	if err := s.store.SaveDataset(dataset); err != nil {
		return domain.Dataset{}, err
	}
	return dataset, nil
}

func enrichDatasetVersionView(version *domain.DatasetVersion) {
	if version == nil {
		return
	}
	version.CleanStatus = cleanStatus(*version)
	if cleanedRef := cleanArtifactRef(*version); cleanedRef != "" {
		version.CleanURI = &cleanedRef
		version.CleanedRef = &cleanedRef
		if version.Metadata != nil {
			version.Metadata["clean_uri"] = cleanedRef
			version.Metadata["cleaned_ref"] = cleanedRef
		}
	}
	if cleanedAt, ok := metadataTime(version.Metadata, "cleaned_at"); ok {
		version.CleanedAt = &cleanedAt
	}
	version.CleanSummary = buildCleanSummary(version.Metadata)
	// silverone 2026-05-28 (β2 cleanup PR2) — PrepareSummary 필드 제거.
}

func (s *DatasetService) attachDatasetVersionArtifacts(version *domain.DatasetVersion) error {
	if version == nil {
		return nil
	}
	artifacts, err := s.store.ListDatasetVersionArtifacts(version.ProjectID, version.DatasetVersionID)
	if err != nil {
		return err
	}
	version.Artifacts = artifacts
	version.BuildStages = buildDatasetVersionStages(*version, version.BuildJobs)
	return nil
}

func buildCleanSummary(metadata map[string]any) *domain.DatasetCleanSummary {
	if len(metadata) == 0 {
		return nil
	}
	raw, ok := metadata["clean_summary"].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	return &domain.DatasetCleanSummary{
		InputRowCount:         intValueOrZero(raw["input_row_count"]),
		OutputRowCount:        intValueOrZero(raw["output_row_count"]),
		KeptCount:             intValueOrZero(raw["kept_count"]),
		DroppedCount:          intValueOrZero(raw["dropped_count"]),
		DedupedCount:          intValueOrZero(raw["deduped_count"]),
		SkippedRowCount:       intValueOrZero(raw["skipped_row_count"]),
		TextColumn:            strings.TrimSpace(anyStringValue(raw["text_column"])),
		TextColumns:           anyStringList(raw["text_columns"]),
		TextJoiner:            anyStringValue(raw["text_joiner"]),
		SourceInputCharCount:  intValueOrZero(raw["source_input_char_count"]),
		CleanedInputCharCount: intValueOrZero(raw["cleaned_input_char_count"]),
		CleanReducedCharCount: intValueOrZero(raw["clean_reduced_char_count"]),
		CleanRegexRuleHits:    intMapValue(raw["clean_regex_rule_hits"]),
	}
}

// silverone 2026-05-28 (β2 cleanup PR2) — buildPrepareSummary 제거. β2로
// prepare 단계 사라져 호출처 0 + DatasetPrepareSummary type 자체 제거됨.
// silverone 2026-06-04 (ADR-018 β2 residue cleanup) — datasetSourceForUnstructured도
// 제거된 embedding/cluster derive helper에서만 쓰여 함께 삭제.

func defaultPrepareRequired(dataType string, value *bool) bool {
	if value != nil {
		return *value
	}
	return false
}

func requiresClean(version domain.DatasetVersion) bool {
	switch version.DataType {
	case "unstructured", "mixed", "both":
		return cleanStatus(version) != "not_applicable"
	default:
		return false
	}
}

// silverone 2026-05-28 (β2 cleanup PR2) — requiresPrepare /
// requiresSentiment / requiresEmbedding은 β2로 모두 dead. 호출처 stub
// (datasetBuildTypePrepare/Sentiment/Embedding stage 자체가 not_applicable).
// stage view 정리(PR3) 시 함수 자체 제거.
func requiresPrepare(_ domain.DatasetVersion) bool { return false }

func cleanStatus(version domain.DatasetVersion) string {
	status := strings.TrimSpace(metadataString(version.Metadata, "clean_status", ""))
	if status != "" {
		return status
	}
	status = strings.TrimSpace(version.CleanStatus)
	if status != "" {
		return status
	}
	switch version.DataType {
	case "unstructured", "mixed", "both":
		return "not_requested"
	default:
		return "not_applicable"
	}
}

// 5/6 endpoint 통일 후 단수 endpoint(/sentiment, /embeddings, /cluster)가
// 비동기 + jobs row 패턴으로 통합되면서 의존성 precondition을 백엔드에서
// 검사하기 위한 helper. 옛날엔 호출자(스크립트/프론트)가 wait_for_status로
// 보장했는데 잘못 호출 시 silent fail로 빠지던 부채 해소.

// silverone 2026-05-28 (β2 cleanup PR2) — struct 필드 제거 후 metadata만 사용.
func prepareStatus(version domain.DatasetVersion) string {
	return strings.TrimSpace(metadataString(version.Metadata, "prepare_status", ""))
}

func isCleanReady(version domain.DatasetVersion) bool {
	return cleanStatus(version) == "ready" && cleanArtifactRef(version) != ""
}

// silverone 2026-05-28 (β2 cleanup PR2) — β2 dead stage. struct 필드 제거.
// stage 자체가 사용 안 되므로 항상 false.
func isPrepareReady(_ domain.DatasetVersion) bool      { return false }
func embeddingBuildReady(_ domain.DatasetVersion) bool { return false }

func datasetClusterReady(version domain.DatasetVersion) bool {
	if strings.TrimSpace(metadataString(version.Metadata, "cluster_status", "")) != "ready" {
		return false
	}
	return strings.TrimSpace(metadataString(version.Metadata, "cluster_ref", "")) != ""
}

func resolveCleanArtifact(version domain.DatasetVersion) (string, string, error) {
	cleanedRef := cleanArtifactRef(version)
	if cleanStatus(version) != "ready" || cleanedRef == "" {
		return "", "", ErrInvalidArgument{Message: "clean artifact is not ready"}
	}
	cleanFormat := strings.TrimSpace(metadataString(version.Metadata, "cleaned_format", ""))
	if cleanFormat == "" {
		cleanFormat = strings.TrimSpace(metadataString(version.Metadata, "clean_format", ""))
	}
	if cleanFormat == "" {
		cleanFormat = inferArtifactFormat(cleanedRef, "parquet")
	}
	return cleanedRef, cleanFormat, nil
}

func cleanArtifactRef(version domain.DatasetVersion) string {
	if version.CleanURI != nil && strings.TrimSpace(*version.CleanURI) != "" {
		return strings.TrimSpace(*version.CleanURI)
	}
	if version.CleanedRef != nil && strings.TrimSpace(*version.CleanedRef) != "" {
		return strings.TrimSpace(*version.CleanedRef)
	}
	if ref := strings.TrimSpace(metadataString(version.Metadata, "clean_uri", "")); ref != "" {
		return ref
	}
	return strings.TrimSpace(metadataString(version.Metadata, "cleaned_ref", ""))
}

func artifactString(artifact map[string]any, key string) string {
	if artifact == nil {
		return ""
	}
	value, ok := artifact[key]
	if !ok || value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

func artifactInt(artifact map[string]any, key string) (int, bool) {
	if artifact == nil {
		return 0, false
	}
	value, ok := artifact[key]
	if !ok || value == nil {
		return 0, false
	}
	return anyToInt(value)
}

func inferArtifactFormat(path string, fallback string) string {
	normalized := strings.ToLower(strings.TrimSpace(path))
	switch {
	case strings.HasSuffix(normalized, ".parquet"):
		return "parquet"
	case strings.HasSuffix(normalized, ".jsonl"):
		return "jsonl"
	default:
		return fallback
	}
}

func anyToInt(value any) (int, bool) {
	switch typed := value.(type) {
	case int:
		return typed, true
	case int32:
		return int(typed), true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	default:
		return 0, false
	}
}

func intValueOrZero(value any) int {
	if converted, ok := anyToInt(value); ok {
		return converted
	}
	return 0
}

func intMapValue(value any) map[string]int {
	source, ok := value.(map[string]any)
	if !ok || len(source) == 0 {
		return nil
	}
	result := make(map[string]int, len(source))
	for key, item := range source {
		if count, ok := anyToInt(item); ok {
			result[key] = count
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func mergeStringAny(base, overlay map[string]any) map[string]any {
	merged := map[string]any{}
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range overlay {
		merged[key] = value
	}
	return merged
}

func metadataNestedString(metadata map[string]any, key, field string) string {
	if metadata == nil {
		return ""
	}
	nested, ok := metadata[key].(map[string]any)
	if !ok {
		return ""
	}
	value, ok := nested[field]
	if !ok {
		return ""
	}
	return strings.TrimSpace(fmt.Sprintf("%v", value))
}

// ── 축제 메타데이터(#31) ──────────────────────────────────────────────────────
// project.metadata.festival = {name, periods:[{year, festival_start, festival_end,
//   before_days?, after_days?}]}.
// 보고서 분석 개요(분석 대상/기간)와 후속 채팅 날짜 해석의 단일 source. 물리 테이블 없이
// project.metadata 인라인 저장(dataset.metadata.taxonomy_id와 동일 패턴).
//
// 데이터 모델(2026-07-01 재설계): 연도별로 "축제기간(during)" start~end만 필수 입력하고,
// 축제 전/후는 명시 날짜가 아니라 ±N일(before_days/after_days) 창으로 파생한다. N을 안 주면
// 전/후는 데이터 기준 개방형(축제기간 앞/뒤 전부)이다. before/during/after 날짜 파생은
// report engine(analysisPeriodsView)이 담당하고, 저장 계약엔 원본(during + N)만 둔다.
//
// 축제(=period 단위 enum)의 화면 표기(축제전/기간/후)는 프론트가 담당.

const (
	festivalPeriodBefore = "before"
	festivalPeriodDuring = "during"
	festivalPeriodAfter  = "after"
)

// normalizeProjectMetadata — 프로젝트 메타데이터를 정규화한다. 현재는 festival만 검증한다
// (있을 때만). 다른 key는 그대로 통과. nil이면 빈 map.
func normalizeProjectMetadata(meta map[string]any) (map[string]any, error) {
	if meta == nil {
		return map[string]any{}, nil
	}
	out := map[string]any{}
	for k, v := range meta {
		out[k] = v
	}
	if raw, ok := out["festival"]; ok && raw != nil {
		normalized, err := normalizeFestivalMetadata(raw)
		if err != nil {
			return nil, err
		}
		out["festival"] = normalized
	}
	return out, nil
}

// normalizeFestivalMetadata — festival 메타데이터를 검증·정규화한다. name 필수, periods는
// 0개 이상(연도별 점진 입력 허용). 연도별 축제기간(festival_start~festival_end) YYYY-MM-DD
// 검증 + start<=end + 날짜 연도 일치 + 연도 중복 금지. before_days/after_days는 선택(>=1일
// 때만 저장, 그 외 미설정=개방형). 실패 시 ErrInvalidArgument.
func normalizeFestivalMetadata(raw any) (map[string]any, error) {
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, ErrInvalidArgument{Message: "festival must be an object"}
	}
	name := ""
	if v, ok := obj["name"].(string); ok {
		name = strings.TrimSpace(v)
	}
	if name == "" {
		return nil, ErrInvalidArgument{Message: "festival.name is required"}
	}

	rawPeriods, _ := obj["periods"].([]any)
	periods := make([]map[string]any, 0, len(rawPeriods))
	seenYear := map[int]bool{}
	for i, rp := range rawPeriods {
		pm, ok := rp.(map[string]any)
		if !ok {
			return nil, ErrInvalidArgument{Message: fmt.Sprintf("festival.periods[%d] must be an object", i)}
		}
		year, err := normalizeFestivalYear(pm["year"])
		if err != nil {
			return nil, ErrInvalidArgument{Message: fmt.Sprintf("festival.periods[%d].year: %s", i, err.Error())}
		}
		if seenYear[year] {
			return nil, ErrInvalidArgument{Message: fmt.Sprintf("festival.periods: year %d is duplicated (one entry per year)", year)}
		}
		seenYear[year] = true

		start, startT, err := normalizeYMD(pm["festival_start"])
		if err != nil {
			return nil, ErrInvalidArgument{Message: fmt.Sprintf("festival.periods[%d].festival_start: %s", i, err.Error())}
		}
		end, endT, err := normalizeYMD(pm["festival_end"])
		if err != nil {
			return nil, ErrInvalidArgument{Message: fmt.Sprintf("festival.periods[%d].festival_end: %s", i, err.Error())}
		}
		if endT.Before(startT) {
			return nil, ErrInvalidArgument{Message: fmt.Sprintf("festival.periods[%d]: festival_end must be >= festival_start", i)}
		}
		if startT.Year() != year || endT.Year() != year {
			return nil, ErrInvalidArgument{Message: fmt.Sprintf("festival.periods[%d]: festival_start/end year must match year %d", i, year)}
		}

		entry := map[string]any{
			"year":           year,
			"festival_start": start,
			"festival_end":   end,
		}
		beforeDays, err := normalizeFestivalDays(pm["before_days"])
		if err != nil {
			return nil, ErrInvalidArgument{Message: fmt.Sprintf("festival.periods[%d].before_days: %s", i, err.Error())}
		}
		if beforeDays > 0 {
			entry["before_days"] = beforeDays
		}
		afterDays, err := normalizeFestivalDays(pm["after_days"])
		if err != nil {
			return nil, ErrInvalidArgument{Message: fmt.Sprintf("festival.periods[%d].after_days: %s", i, err.Error())}
		}
		if afterDays > 0 {
			entry["after_days"] = afterDays
		}
		periods = append(periods, entry)
	}
	return map[string]any{"name": name, "periods": periods}, nil
}

// normalizeFestivalYear — JSON number(float64)/int/문자열("2025"/"2025년")을 int 연도로.
func normalizeFestivalYear(v any) (int, error) {
	switch t := v.(type) {
	case float64:
		return int(t), nil
	case int:
		return t, nil
	case int64:
		return int(t), nil
	case string:
		s := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(t), "년"))
		n, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("invalid year %q", t)
		}
		return n, nil
	default:
		return 0, fmt.Errorf("year is required")
	}
}

// normalizeFestivalDays — 전/후 ±N일. 없음/null/0 이하는 0(미설정=개방형)으로, 그 외 음수는
// 오류. JSON number(float64)/int/문자열("3") 허용.
func normalizeFestivalDays(v any) (int, error) {
	if v == nil {
		return 0, nil
	}
	var n int
	switch t := v.(type) {
	case float64:
		n = int(t)
	case int:
		n = t
	case int64:
		n = int(t)
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, nil
		}
		parsed, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("must be a non-negative integer, got %q", t)
		}
		n = parsed
	default:
		return 0, fmt.Errorf("must be a non-negative integer")
	}
	if n < 0 {
		return 0, fmt.Errorf("must be a non-negative integer, got %d", n)
	}
	return n, nil
}

// normalizeYMD — YYYY-MM-DD 문자열을 검증하고 정규화 문자열 + time을 돌려준다.
func normalizeYMD(v any) (string, time.Time, error) {
	s, _ := v.(string)
	s = strings.TrimSpace(s)
	if s == "" {
		return "", time.Time{}, fmt.Errorf("date is required (YYYY-MM-DD)")
	}
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("invalid date %q (expected YYYY-MM-DD)", s)
	}
	return t.Format("2006-01-02"), t, nil
}

func ensureParentDir(path string) error {
	dir := filepath.Dir(strings.TrimSpace(path))
	if dir == "" || dir == "." {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func sanitizeFilename(value string) string {
	trimmed := strings.TrimSpace(filepath.Base(value))
	if trimmed == "" || trimmed == "." || trimmed == string(filepath.Separator) {
		return ""
	}
	sanitized := strings.Map(func(r rune) rune {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			return r
		case r == '.', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, trimmed)
	sanitized = strings.Trim(sanitized, "._")
	if sanitized == "" {
		return ""
	}
	return sanitized
}
