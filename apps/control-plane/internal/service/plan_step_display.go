package service

import (
	"fmt"
	"sort"
	"strings"
)

// silverone 2026-06-01 (plan step display projection) — projectAnalyzePlan에서
// 각 plan step에 사용자 화면용 `display = {label, expression}`을 덧붙이는
// 빌더. plan body 자체에는 변경 X — projection 단계에서만 합성된다.
//
// 설계 원칙:
//   - 프론트는 display.label / display.expression을 우선 표시.
//     display가 없으면 raw params JSON fallback.
//   - raw params는 그대로 유지 (고급 보기용).
//   - expression은 SQL-like 직관 표현. 실제 executor SQL과는 무관 —
//     debug 노출 금지 정책 (사용자 결정 2026-06-01).
//   - label은 skill 종류별 한국어 일반명. step.id가 의미 있으면 보조로 활용
//     하지 않는다 (영문 식별자 → 한국어 자동 번역 부정확).

// buildStepDisplay — plan step 1개의 display 객체 생성.
// 입력은 raw step map (id / skill / params 포함). 알 수 없는 skill 또는
// params가 깨져 있으면 nil 반환 — 프론트는 raw JSON fallback으로 떨어진다.
func buildStepDisplay(step map[string]any) map[string]any {
	skill, _ := step["skill"].(string)
	params, _ := step["params"].(map[string]any)
	if skill == "" {
		return nil
	}
	switch skill {
	case "filter":
		return displayFilter(params)
	case "join":
		return displayJoin(params)
	case "aggregate":
		return displayAggregate(params)
	case "compare":
		return displayCompare(params)
	case "calculate":
		return displayCalculate(params)
	case "sort":
		return displaySort(params)
	case "present":
		return displayPresent(params)
	case "summarize":
		return displaySummarize(params)
	}
	return nil
}

func displayFilter(p map[string]any) map[string]any {
	column := stringParam(p, "column")
	op := stringParam(p, "operator")
	value := p["value"]
	if column == "" {
		return nil
	}
	expr := "WHERE " + column + " " + filterOperatorToSQL(op, value)
	return map[string]any{
		"label":      "조건 필터",
		"expression": expr,
	}
}

func filterOperatorToSQL(op string, value any) string {
	switch op {
	case "eq":
		return "= " + formatLiteral(value)
	case "neq":
		return "!= " + formatLiteral(value)
	case "gt":
		return "> " + formatLiteral(value)
	case "gte":
		return ">= " + formatLiteral(value)
	case "lt":
		return "< " + formatLiteral(value)
	case "lte":
		return "<= " + formatLiteral(value)
	case "in":
		return "IN " + formatList(value)
	case "not_in":
		return "NOT IN " + formatList(value)
	case "contains":
		return "LIKE " + formatLiteral(value)
	case "between":
		return "BETWEEN " + formatBetween(value)
	case "is_null":
		return "IS NULL"
	case "not_null":
		return "IS NOT NULL"
	}
	return op + " " + formatLiteral(value)
}

func displayJoin(p map[string]any) map[string]any {
	left := stringParam(p, "left")
	right := stringParam(p, "right")
	how := stringParam(p, "how")
	if how == "" {
		how = "inner"
	}
	on := stringListParam(p, "on")
	if left == "" || right == "" {
		return nil
	}
	expr := fmt.Sprintf("%s %s JOIN %s", left, strings.ToUpper(how), right)
	if len(on) > 0 {
		expr += " ON " + strings.Join(on, ", ")
	}
	return map[string]any{
		"label":      "데이터 연결",
		"expression": expr,
	}
}

func displayAggregate(p map[string]any) map[string]any {
	groupBy := stringListParam(p, "group_by")
	metrics, _ := p["metrics"].([]any)
	parts := make([]string, 0, 2)
	if len(groupBy) > 0 {
		parts = append(parts, "GROUP BY "+strings.Join(groupBy, ", "))
	}
	if len(metrics) > 0 {
		metricExprs := make([]string, 0, len(metrics))
		for _, m := range metrics {
			metric, ok := m.(map[string]any)
			if !ok {
				continue
			}
			fn := strings.ToUpper(stringParam(metric, "function"))
			col := stringParam(metric, "column")
			name := stringParam(metric, "name")
			if fn == "" {
				continue
			}
			callExpr := fn + "(" + col + ")"
			if fn == "COUNT" && col == "" {
				callExpr = "COUNT(*)"
			}
			if name != "" {
				callExpr += " AS " + name
			}
			metricExprs = append(metricExprs, callExpr)
		}
		if len(metricExprs) > 0 {
			parts = append(parts, strings.Join(metricExprs, ", "))
		}
	}
	if len(parts) == 0 {
		return nil
	}
	label := "집계"
	if len(groupBy) > 0 {
		label = groupBy[0] + "별 집계"
	}
	return map[string]any{
		"label":      label,
		"expression": strings.Join(parts, " · "),
	}
}

func displayCompare(p map[string]any) map[string]any {
	left := stringParam(p, "left")
	right := stringParam(p, "right")
	leftLabel := stringParam(p, "left_label")
	rightLabel := stringParam(p, "right_label")
	joinKey := stringListParam(p, "join_key")
	if left == "" || right == "" {
		return nil
	}
	leftName := left
	if leftLabel != "" {
		leftName = leftLabel
	}
	rightName := right
	if rightLabel != "" {
		rightName = rightLabel
	}
	expr := fmt.Sprintf("COMPARE %s vs %s", leftName, rightName)
	if len(joinKey) > 0 {
		expr += " ON " + strings.Join(joinKey, ", ")
	}
	return map[string]any{
		"label":      "두 결과 비교",
		"expression": expr,
	}
}

func displayCalculate(p map[string]any) map[string]any {
	expressions, _ := p["expressions"].([]any)
	if len(expressions) == 0 {
		return nil
	}
	exprs := make([]string, 0, len(expressions))
	label := "값 계산"
	for _, item := range expressions {
		e, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := stringParam(e, "name")
		op := stringParam(e, "operation")
		left := stringParam(e, "left")
		right := stringParam(e, "right")
		switch op {
		case "ratio":
			label = "비율 계산"
			expr := fmt.Sprintf("%s / %s * 100", nonEmpty(left, "left"), nonEmpty(right, "right"))
			if name != "" {
				expr = name + " = " + expr
			}
			exprs = append(exprs, expr)
		case "percent_change":
			label = "증감률 계산"
			expr := fmt.Sprintf("(%s - %s) / %s * 100", nonEmpty(right, "right"), nonEmpty(left, "left"), nonEmpty(left, "left"))
			if name != "" {
				expr = name + " = " + expr
			}
			exprs = append(exprs, expr)
		case "share_of_total":
			// silverone 2026-06-02 — 전체(또는 partition 그룹) 합 대비 비중.
			label = "비중 계산"
			value := stringParam(e, "value")
			denom := "전체 합계"
			if parts := stringListParam(e, "partition_by"); len(parts) > 0 {
				denom = strings.Join(parts, ", ") + "별 합계"
			}
			expr := fmt.Sprintf("%s / %s * 100", nonEmpty(value, "value"), denom)
			if name != "" {
				expr = name + " = " + expr
			}
			exprs = append(exprs, expr)
		case "add", "subtract", "multiply", "divide":
			symbol := map[string]string{
				"add": "+", "subtract": "-", "multiply": "*", "divide": "/",
			}[op]
			expr := fmt.Sprintf("%s %s %s", nonEmpty(left, "left"), symbol, nonEmpty(right, "right"))
			if name != "" {
				expr = name + " = " + expr
			}
			exprs = append(exprs, expr)
		default:
			if name != "" {
				exprs = append(exprs, name+" = "+op)
			} else {
				exprs = append(exprs, op)
			}
		}
	}
	if len(exprs) == 0 {
		return nil
	}
	return map[string]any{
		"label":      label,
		"expression": strings.Join(exprs, ", "),
	}
}

func displaySort(p map[string]any) map[string]any {
	by := stringListParam(p, "by")
	order := stringParam(p, "order")
	if order == "" {
		order = "desc"
	}
	limit := p["limit"]
	if len(by) == 0 {
		return nil
	}
	expr := "ORDER BY " + strings.Join(by, ", ") + " " + strings.ToUpper(order)
	if l, ok := limit.(float64); ok && l > 0 {
		expr += fmt.Sprintf(" LIMIT %d", int(l))
	}
	return map[string]any{
		"label":      "정렬",
		"expression": expr,
	}
}

func displayPresent(p map[string]any) map[string]any {
	format := stringParam(p, "format")
	if format == "" {
		format = "table"
	}
	title := stringParam(p, "title")
	expr := strings.ToUpper(format)
	if title != "" {
		expr = strings.ToUpper(format) + ": " + title
	}
	if l, ok := p["limit"].(float64); ok && l > 0 {
		expr += fmt.Sprintf(" (LIMIT %d)", int(l))
	}
	return map[string]any{
		"label":      "결과 표시",
		"expression": expr,
	}
}

func displaySummarize(p map[string]any) map[string]any {
	focus := stringParam(p, "focus")
	expr := "SUMMARIZE"
	if focus != "" {
		expr += " focus=" + focus
	}
	return map[string]any{
		"label":      "자연어 요약",
		"expression": expr,
	}
}

// ===== helpers =====

func stringParam(p map[string]any, key string) string {
	if p == nil {
		return ""
	}
	v, ok := p[key].(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(v)
}

func stringListParam(p map[string]any, key string) []string {
	if p == nil {
		return nil
	}
	raw, ok := p[key].([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		if s, ok := item.(string); ok {
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

func formatLiteral(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case bool:
		if x {
			return "TRUE"
		}
		return "FALSE"
	case float64:
		if x == float64(int64(x)) {
			return fmt.Sprintf("%d", int64(x))
		}
		return fmt.Sprintf("%g", x)
	}
	return fmt.Sprintf("%v", v)
}

func formatList(v any) string {
	if arr, ok := v.([]any); ok {
		parts := make([]string, 0, len(arr))
		for _, item := range arr {
			parts = append(parts, formatLiteral(item))
		}
		// determinism — 사용자 화면 안정성.
		sort.Strings(parts)
		return "(" + strings.Join(parts, ", ") + ")"
	}
	return "(" + formatLiteral(v) + ")"
}

func formatBetween(v any) string {
	if arr, ok := v.([]any); ok && len(arr) == 2 {
		return formatLiteral(arr[0]) + " AND " + formatLiteral(arr[1])
	}
	return formatLiteral(v)
}

func nonEmpty(s, fallback string) string {
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return s
}
