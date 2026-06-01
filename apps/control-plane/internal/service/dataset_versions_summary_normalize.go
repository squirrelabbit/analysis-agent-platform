package service

import "encoding/json"

// silverone 2026-05-28 (C 옵션) — version-detail의 stage summary를 build-detail
// 응답과 같은 normalized shape으로 통일한다. Python worker가 metadata에 저장한
// raw count key(tier_counts / aspect_counts / sentiment_counts / clause_count)
// 만 정리하고, 운영·감사용 부수 필드(applied / model / prompt_version /
// taxonomy_id / total_*_tokens 등)는 그대로 보존한다.

// normalizeDocGenuinenessSummary — tier_counts → genuineness + total.
// total 우선순위: processed_row_count(있으면) > tier_counts 합.
func normalizeDocGenuinenessSummary(raw any) any {
	m, ok := raw.(map[string]any)
	if !ok {
		return raw
	}
	out := make(map[string]any, len(m)+2)
	var genuineness map[string]any
	for k, v := range m {
		if k == "tier_counts" {
			if tc, ok := v.(map[string]any); ok {
				genuineness = normalizeCountMap(tc)
			}
			continue
		}
		out[k] = v
	}
	if genuineness != nil {
		out["genuineness"] = genuineness
	}
	if prc, ok := out["processed_row_count"]; ok {
		if i, ok := summaryCountToInt(prc); ok {
			out["total"] = i
		}
	} else if genuineness != nil {
		if sum, ok := sumCountMap(genuineness); ok {
			out["total"] = sum
		}
	}
	return out
}

// normalizeClauseLabelSummary — aspect_counts / sentiment_counts / clause_count
// 를 aspect / sentiment / total로 옮긴다.
func normalizeClauseLabelSummary(raw any) any {
	m, ok := raw.(map[string]any)
	if !ok {
		return raw
	}
	out := make(map[string]any, len(m)+3)
	var aspect, sentiment map[string]any
	var clauseCount any
	var hasClauseCount bool
	for k, v := range m {
		switch k {
		case "aspect_counts":
			if a, ok := v.(map[string]any); ok {
				aspect = normalizeCountMap(a)
			}
			continue
		case "sentiment_counts":
			if s, ok := v.(map[string]any); ok {
				sentiment = normalizeCountMap(s)
			}
			continue
		case "clause_count":
			clauseCount = v
			hasClauseCount = true
			continue
		}
		out[k] = v
	}
	if aspect != nil {
		out["aspect"] = aspect
	}
	if sentiment != nil {
		out["sentiment"] = sentiment
	}
	if hasClauseCount {
		if i, ok := summaryCountToInt(clauseCount); ok {
			out["total"] = i
		}
	}
	return out
}

// normalizeCountMap — count map의 값을 int로 정규화. build-detail의
// aggregateGroupedCounts return 타입(map[string]int)과 직렬화 결과를 일치시킨다.
func normalizeCountMap(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		if i, ok := summaryCountToInt(v); ok {
			out[k] = i
			continue
		}
		out[k] = v
	}
	return out
}

// sumCountMap — 모든 값이 int 변환 가능하면 합을 반환. 하나라도 변환 실패면
// (0,false).
func sumCountMap(m map[string]any) (int, bool) {
	sum := 0
	for _, v := range m {
		i, ok := summaryCountToInt(v)
		if !ok {
			return 0, false
		}
		sum += i
	}
	return sum, true
}

// summaryCountToInt — JSON unmarshal / postgres jsonb scan 결과의 다양한 숫자
// 표현을 int로 변환.
func summaryCountToInt(v any) (int, bool) {
	switch x := v.(type) {
	case int:
		return x, true
	case int64:
		return int(x), true
	case int32:
		return int(x), true
	case float64:
		return int(x), true
	case float32:
		return int(x), true
	case json.Number:
		i, err := x.Int64()
		if err != nil {
			return 0, false
		}
		return int(i), true
	}
	return 0, false
}
