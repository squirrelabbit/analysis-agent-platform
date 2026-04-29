package executionresult

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
	skillruntime "analysis-support-platform/control-plane/internal/skills"
)

func BuildV1(execution domain.ExecutionSummary) domain.ExecutionResultV1 {
	decodedArtifacts := decodeExecutionArtifacts(execution.Artifacts)
	primaryArtifactKey, primaryArtifact := selectPrimaryExecutionArtifact(decodedArtifacts, execution.Plan)

	result := domain.ExecutionResultV1{
		SchemaVersion: "execution-result-v1",
		Status:        execution.Status,
		StepResults:   buildExecutionStepResultsV1(execution, decodedArtifacts),
		Profile:       execution.ProfileSnapshot,
	}
	if usageSummary := buildArtifactUsageSummary(execution.Artifacts); len(usageSummary) > 0 {
		result.UsageSummary = usageSummary
	}
	if primaryArtifactKey != "" {
		result.PrimaryArtifactKey = stringPointer(primaryArtifactKey)
	}
	if primarySkillName := skillruntime.CanonicalSkillName(artifactStringValue(primaryArtifact["skill_name"])); primarySkillName != "" {
		result.PrimarySkillName = stringPointer(primarySkillName)
	}
	if answer := buildExecutionAnswerV1(primaryArtifact, decodedArtifacts); answer != nil {
		result.Answer = answer
	}
	if waiting := latestWaitingState(execution.Status, execution.Events); waiting != nil {
		result.Waiting = waiting
	}
	if warnings := collectExecutionWarnings(execution.Status, execution.Events, decodedArtifacts); len(warnings) > 0 {
		result.Warnings = warnings
	}
	return result
}

func decodeExecutionArtifacts(artifacts map[string]string) map[string]map[string]any {
	decoded := make(map[string]map[string]any, len(artifacts))
	for key, raw := range artifacts {
		var artifact map[string]any
		if err := json.Unmarshal([]byte(raw), &artifact); err != nil {
			continue
		}
		decoded[key] = artifact
	}
	return decoded
}

func selectPrimaryExecutionArtifact(decoded map[string]map[string]any, plan domain.SkillPlan) (string, map[string]any) {
	priority := []string{
		"issue_evidence_summary",
		"issue_cluster_summary",
		"issue_taxonomy_summary",
		"issue_sentiment_summary",
		"issue_breakdown_summary",
		"issue_trend_summary",
		"issue_period_compare",
		"structured_kpi_summary",
		"unstructured_issue_summary",
		"semantic_search",
	}
	keys := sortedArtifactKeysFromDecoded(decoded)
	for _, skillName := range priority {
		for _, step := range plan.Steps {
			if !skillruntime.IsAliasFor(step.SkillName, skillName) {
				continue
			}
			key := artifactKeyForStep(keys, step.StepID, skillName)
			if key == "" {
				continue
			}
			return key, decoded[key]
		}
		for _, key := range keys {
			if artifactKeyMatchesSkillName(key, skillName) {
				return key, decoded[key]
			}
		}
	}
	for _, key := range keys {
		return key, decoded[key]
	}
	return "", nil
}

func buildExecutionAnswerV1(primaryArtifact map[string]any, decoded map[string]map[string]any) *domain.ExecutionResultAnswer {
	if len(primaryArtifact) == 0 {
		return nil
	}
	answer := &domain.ExecutionResultAnswer{
		Summary:           strings.TrimSpace(executionArtifactSummary(primaryArtifact)),
		KeyFindings:       executionArtifactKeyFindings(primaryArtifact),
		Evidence:          executionArtifactEvidence(primaryArtifact),
		FollowUpQuestions: executionArtifactStringList(primaryArtifact["follow_up_questions"]),
		SelectionSource:   strings.TrimSpace(artifactStringValue(primaryArtifact["selection_source"])),
		CitationMode:      strings.TrimSpace(artifactStringValue(primaryArtifact["citation_mode"])),
	}
	if answer.Summary == "" {
		answer.Summary = "실행은 완료됐지만 대표 요약을 생성하지 못했습니다."
	}
	if len(answer.Evidence) == 0 {
		if evidenceKey, evidenceArtifact := selectPrimaryBySkills(decoded, "issue_evidence_summary"); evidenceKey != "" {
			answer.Evidence = executionArtifactEvidence(evidenceArtifact)
			if answer.SelectionSource == "" {
				answer.SelectionSource = strings.TrimSpace(artifactStringValue(evidenceArtifact["selection_source"]))
			}
			if answer.CitationMode == "" {
				answer.CitationMode = strings.TrimSpace(artifactStringValue(evidenceArtifact["citation_mode"]))
			}
			if len(answer.FollowUpQuestions) == 0 {
				answer.FollowUpQuestions = executionArtifactStringList(evidenceArtifact["follow_up_questions"])
			}
		}
	}
	if len(answer.KeyFindings) == 0 {
		answer.KeyFindings = deriveExecutionFindings(primaryArtifact)
	}
	return answer
}

func buildExecutionStepResultsV1(execution domain.ExecutionSummary, decoded map[string]map[string]any) []domain.ExecutionStepResultV1 {
	results := make([]domain.ExecutionStepResultV1, 0, len(execution.Plan.Steps))
	keys := sortedArtifactKeysFromDecoded(decoded)
	for _, step := range execution.Plan.Steps {
		result := domain.ExecutionStepResultV1{
			StepID:    step.StepID,
			SkillName: step.SkillName,
			Status:    "pending",
		}
		key := artifactKeyForStep(keys, step.StepID, step.SkillName)
		if key != "" {
			artifact := decoded[key]
			result.Status = "completed"
			result.ArtifactKey = stringPointer(key)
			result.Summary = executionArtifactSummary(artifact)
			result.Usage = executionUsageMap(artifact)
			if artifactRef := firstArtifactRef(artifact); artifactRef != "" {
				result.ArtifactRef = stringPointer(artifactRef)
			}
			if selectionMode := strings.TrimSpace(artifactStringValue(artifact["selection_source"])); selectionMode != "" {
				result.SelectionMode = selectionMode
			}
			if warnings := artifactWarnings(artifact); len(warnings) > 0 {
				result.Warnings = warnings
			}
		} else if execution.Status == "failed" {
			result.Status = "missing"
		}
		results = append(results, result)
	}
	return results
}

func latestWaitingState(status string, events []domain.ExecutionEvent) *domain.ExecutionWaitingState {
	if strings.TrimSpace(status) != "waiting" {
		return nil
	}
	for index := len(events) - 1; index >= 0; index-- {
		event := events[index]
		if event.EventType != "WORKFLOW_WAITING" {
			continue
		}
		waitingFor := strings.TrimSpace(artifactStringValue(event.Payload["waiting_for"]))
		reason := strings.TrimSpace(artifactStringValue(event.Payload["reason"]))
		if waitingFor == "" && reason == "" {
			return nil
		}
		return &domain.ExecutionWaitingState{
			WaitingFor: waitingFor,
			Reason:     reason,
		}
	}
	return nil
}

func collectExecutionWarnings(status string, events []domain.ExecutionEvent, decoded map[string]map[string]any) []string {
	warnings := make([]string, 0)
	for _, event := range events {
		switch event.EventType {
		case "WORKFLOW_FAILED":
			if message := strings.TrimSpace(event.Message); message != "" {
				warnings = append(warnings, message)
			}
			if errText := strings.TrimSpace(artifactStringValue(event.Payload["error"])); errText != "" {
				warnings = append(warnings, errText)
			}
		case "WORKFLOW_WAITING":
			if strings.TrimSpace(status) != "waiting" {
				continue
			}
			waitingFor := strings.TrimSpace(artifactStringValue(event.Payload["waiting_for"]))
			reason := strings.TrimSpace(artifactStringValue(event.Payload["reason"]))
			if waitingFor != "" || reason != "" {
				warnings = append(warnings, strings.TrimSpace("waiting_for="+waitingFor+" "+reason))
			}
		case "WORKFLOW_COMPLETED":
			for _, note := range executionArtifactStringList(event.Payload["structured_notes"]) {
				lower := strings.ToLower(note)
				if strings.Contains(lower, "fallback") || strings.Contains(lower, "warning") || strings.Contains(lower, "error") || strings.Contains(lower, "failed") {
					warnings = append(warnings, note)
				}
			}
		}
	}
	for _, key := range sortedArtifactKeysFromDecoded(decoded) {
		warnings = append(warnings, artifactWarnings(decoded[key])...)
	}
	return uniqueNonEmptyStrings(warnings)
}

func buildArtifactUsageSummary(artifacts map[string]string) map[string]any {
	summary := map[string]any{}
	for _, raw := range artifacts {
		var decoded map[string]any
		if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
			continue
		}
		usage, ok := decoded["usage"].(map[string]any)
		if !ok || len(usage) == 0 {
			continue
		}
		summary = mergeExecutionUsage(summary, usage)
	}
	if len(summary) == 0 {
		return nil
	}
	return summary
}

func mergeExecutionUsage(left, right map[string]any) map[string]any {
	if len(left) == 0 && len(right) == 0 {
		return nil
	}
	result := map[string]any{}
	for key, value := range left {
		result[key] = value
	}
	for key, value := range right {
		switch key {
		case "request_count", "input_tokens", "output_tokens", "total_tokens", "prompt_tokens", "input_text_count", "vector_count":
			result[key] = usageIntValue(result[key]) + usageIntValue(value)
		case "estimated_cost_usd":
			result[key] = usageRoundCost(usageFloatValue(result[key]) + usageFloatValue(value))
		case "provider", "model", "operation", "cost_estimation_status":
			existing := strings.TrimSpace(artifactStringValue(result[key]))
			incoming := strings.TrimSpace(artifactStringValue(value))
			if existing == "" {
				result[key] = incoming
			} else if incoming == "" || existing == incoming {
				result[key] = existing
			} else {
				result[key] = "mixed"
			}
		default:
			if _, ok := result[key]; !ok {
				result[key] = value
			}
		}
	}
	return result
}

func executionArtifactSummary(artifact map[string]any) string {
	if len(artifact) == 0 {
		return ""
	}
	if summaryText := strings.TrimSpace(artifactStringValue(artifact["summary"])); summaryText != "" && summaryText != "map[]" {
		if _, ok := artifact["summary"].(map[string]any); !ok {
			return summaryText
		}
	}
	skillName := skillruntime.CanonicalSkillName(artifactStringValue(artifact["skill_name"]))
	summary, _ := artifact["summary"].(map[string]any)
	switch skillName {
	case "issue_cluster_summary":
		label := strings.TrimSpace(artifactStringValue(summary["dominant_cluster_label"]))
		count := usageIntValue(summary["dominant_cluster_count"])
		clusterCount := usageIntValue(summary["cluster_count"])
		if label != "" {
			return fmt.Sprintf("가장 큰 군집은 %s이며 %d건입니다. 전체 군집 수는 %d개입니다.", label, count, clusterCount)
		}
	case "issue_taxonomy_summary":
		label := strings.TrimSpace(artifactStringValue(summary["dominant_taxonomy_label"]))
		if label == "" {
			label = strings.TrimSpace(artifactStringValue(summary["dominant_taxonomy"]))
		}
		count := usageIntValue(summary["dominant_taxonomy_count"])
		if label != "" {
			return fmt.Sprintf("가장 큰 taxonomy는 %s이며 %d건입니다.", label, count)
		}
	case "issue_sentiment_summary":
		label := strings.TrimSpace(artifactStringValue(summary["dominant_label"]))
		count := usageIntValue(summary["dominant_label_count"])
		if label != "" {
			return fmt.Sprintf("지배적인 감성은 %s이며 %d건입니다.", label, count)
		}
	case "issue_breakdown_summary":
		topGroup := strings.TrimSpace(artifactStringValue(summary["top_group"]))
		count := usageIntValue(summary["top_group_count"])
		if topGroup != "" {
			return fmt.Sprintf("최다 그룹은 %s이며 %d건입니다.", topGroup, count)
		}
	case "issue_trend_summary":
		peak := strings.TrimSpace(artifactStringValue(summary["peak_bucket"]))
		count := usageIntValue(summary["peak_count"])
		if peak != "" {
			return fmt.Sprintf("피크 구간은 %s이며 %d건입니다.", peak, count)
		}
	case "issue_period_compare":
		currentCount := usageIntValue(summary["current_count"])
		previousCount := usageIntValue(summary["previous_count"])
		countDelta := usageIntValue(summary["count_delta"])
		return fmt.Sprintf("현재 기간 %d건, 이전 기간 %d건으로 %d건 변화했습니다.", currentCount, previousCount, countDelta)
	case "structured_kpi_summary":
		rowCount := usageIntValue(summary["row_count"])
		metricSum := usageFloatValue(summary["metric_sum"])
		metricAvg := usageFloatValue(summary["metric_avg"])
		return fmt.Sprintf("구조화 KPI %d행을 집계했고 합계 %.2f, 평균 %.2f입니다.", rowCount, metricSum, metricAvg)
	case "semantic_search":
		matches := executionArtifactMapSlice(artifact["matches"], 1)
		if len(matches) > 0 {
			text := strings.TrimSpace(artifactStringValue(matches[0]["text"]))
			if text != "" {
				return fmt.Sprintf("가장 관련 높은 근거는 '%s' 입니다.", text)
			}
		}
	case "document_filter":
		count := usageIntValue(summary["filtered_row_count"])
		inputCount := usageIntValue(summary["input_row_count"])
		return fmt.Sprintf("%d개 행을 선택했습니다. 전체 입력은 %d개였습니다.", count, inputCount)
	case "noun_frequency":
		topTerms := executionArtifactMapSlice(artifact["top_nouns"], 2)
		terms := make([]string, 0, len(topTerms))
		for _, item := range topTerms {
			term := strings.TrimSpace(artifactStringValue(item["term"]))
			if term != "" {
				terms = append(terms, term)
			}
		}
		if len(terms) > 0 {
			return fmt.Sprintf("상위 명사는 %s 입니다.", strings.Join(terms, ", "))
		}
	case "sentence_split":
		documentCount := usageIntValue(summary["document_count"])
		sentenceCount := usageIntValue(summary["sentence_count"])
		return fmt.Sprintf("%d개 문서를 %d개 문장으로 분리했습니다.", documentCount, sentenceCount)
	case "deduplicate_documents":
		canonicalCount := usageIntValue(summary["canonical_row_count"])
		duplicateCount := usageIntValue(summary["duplicate_row_count"])
		return fmt.Sprintf("중복 제거 후 대표 행은 %d개이며 중복 행은 %d개였습니다.", canonicalCount, duplicateCount)
	case "garbage_filter":
		removedCount := usageIntValue(summary["removed_row_count"])
		retainedCount := usageIntValue(summary["retained_row_count"])
		return fmt.Sprintf("가비지 문서 %d건을 제거하고 %d건을 유지했습니다.", removedCount, retainedCount)
	}
	if findings := executionArtifactKeyFindings(artifact); len(findings) > 0 {
		return findings[0]
	}
	return ""
}

func executionArtifactKeyFindings(artifact map[string]any) []string {
	findings := executionArtifactStringList(artifact["key_findings"])
	if len(findings) > 0 {
		return findings
	}
	return deriveExecutionFindings(artifact)
}

func deriveExecutionFindings(artifact map[string]any) []string {
	skillName := skillruntime.CanonicalSkillName(artifactStringValue(artifact["skill_name"]))
	summary, _ := artifact["summary"].(map[string]any)
	findings := make([]string, 0)
	switch skillName {
	case "issue_cluster_summary":
		for _, item := range executionArtifactMapSlice(artifact["clusters"], 3) {
			label := strings.TrimSpace(artifactStringValue(item["label"]))
			count := usageIntValue(item["document_count"])
			if label != "" && count > 0 {
				findings = append(findings, fmt.Sprintf("%s 군집이 %d건입니다.", label, count))
			}
		}
	case "issue_taxonomy_summary":
		for _, item := range executionArtifactMapSlice(artifact["taxonomy_breakdown"], 3) {
			label := strings.TrimSpace(artifactStringValue(item["label"]))
			if label == "" {
				label = strings.TrimSpace(artifactStringValue(item["taxonomy_id"]))
			}
			count := usageIntValue(item["count"])
			if label != "" && count > 0 {
				findings = append(findings, fmt.Sprintf("%s taxonomy가 %d건입니다.", label, count))
			}
		}
	case "issue_sentiment_summary":
		for _, item := range executionArtifactMapSlice(artifact["breakdown"], 3) {
			label := strings.TrimSpace(artifactStringValue(item["sentiment_label"]))
			count := usageIntValue(item["count"])
			if label != "" && count > 0 {
				findings = append(findings, fmt.Sprintf("%s 감성이 %d건입니다.", label, count))
			}
		}
	case "issue_breakdown_summary":
		for _, item := range executionArtifactMapSlice(artifact["breakdown"], 3) {
			group := strings.TrimSpace(artifactStringValue(item["group"]))
			count := usageIntValue(item["count"])
			if group != "" && count > 0 {
				findings = append(findings, fmt.Sprintf("%s 그룹이 %d건입니다.", group, count))
			}
		}
	case "issue_trend_summary":
		if peak := strings.TrimSpace(artifactStringValue(summary["peak_bucket"])); peak != "" {
			findings = append(findings, fmt.Sprintf("피크 구간은 %s입니다.", peak))
		}
	case "issue_period_compare":
		currentCount := usageIntValue(summary["current_count"])
		previousCount := usageIntValue(summary["previous_count"])
		countDelta := usageIntValue(summary["count_delta"])
		findings = append(findings, fmt.Sprintf("현재 %d건, 이전 %d건, 변화량 %d건입니다.", currentCount, previousCount, countDelta))
	case "structured_kpi_summary":
		rowCount := usageIntValue(summary["row_count"])
		findings = append(findings, fmt.Sprintf("집계 대상은 %d행입니다.", rowCount))
	case "noun_frequency":
		for _, item := range executionArtifactMapSlice(artifact["top_nouns"], 3) {
			term := strings.TrimSpace(artifactStringValue(item["term"]))
			count := usageIntValue(item["term_frequency"])
			if term != "" && count > 0 {
				findings = append(findings, fmt.Sprintf("%s 명사가 %d회 등장했습니다.", term, count))
			}
		}
	case "sentence_split":
		sentenceCount := usageIntValue(summary["sentence_count"])
		documentCount := usageIntValue(summary["document_count"])
		if sentenceCount > 0 {
			findings = append(findings, fmt.Sprintf("%d개 문서를 %d개 문장으로 분리했습니다.", documentCount, sentenceCount))
		}
	}
	return uniqueNonEmptyStrings(findings)
}

func executionArtifactEvidence(artifact map[string]any) []map[string]any {
	items := executionArtifactMapSlice(artifact["evidence"], 5)
	if len(items) > 0 {
		return items
	}
	skillName := skillruntime.CanonicalSkillName(artifactStringValue(artifact["skill_name"]))
	switch skillName {
	case "issue_cluster_summary":
		clusters := executionArtifactMapSlice(artifact["clusters"], 1)
		if len(clusters) == 0 {
			return nil
		}
		return executionArtifactMapSlice(clusters[0]["samples"], 3)
	case "issue_sentiment_summary":
		breakdown := executionArtifactMapSlice(artifact["breakdown"], 1)
		if len(breakdown) == 0 {
			return nil
		}
		samples := executionArtifactStringList(breakdown[0]["samples"])
		evidence := make([]map[string]any, 0, len(samples))
		for index, sample := range samples {
			evidence = append(evidence, map[string]any{
				"rank":    index + 1,
				"snippet": sample,
			})
		}
		return evidence
	default:
		return nil
	}
}

func executionUsageMap(artifact map[string]any) map[string]any {
	usage, ok := artifact["usage"].(map[string]any)
	if !ok || len(usage) == 0 {
		return nil
	}
	return usage
}

func firstArtifactRef(artifact map[string]any) string {
	for _, key := range []string{"artifact_ref", "chunk_ref", "embedding_index_ref"} {
		if value := strings.TrimSpace(artifactStringValue(artifact[key])); value != "" {
			return value
		}
	}
	return ""
}

func artifactWarnings(artifact map[string]any) []string {
	warnings := make([]string, 0)
	for _, note := range executionArtifactStringList(artifact["notes"]) {
		lower := strings.ToLower(note)
		if strings.Contains(lower, "fallback") || strings.Contains(lower, "warning") || strings.Contains(lower, "error") || strings.Contains(lower, "failed") {
			warnings = append(warnings, note)
		}
	}
	return warnings
}

func executionArtifactMapSlice(value any, limit int) []map[string]any {
	switch typed := value.(type) {
	case []map[string]any:
		if limit > 0 && len(typed) > limit {
			return typed[:limit]
		}
		return typed
	case []any:
		items := make([]map[string]any, 0, len(typed))
		for _, raw := range typed {
			item, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			items = append(items, item)
			if limit > 0 && len(items) >= limit {
				break
			}
		}
		return items
	default:
		return nil
	}
}

func executionArtifactStringList(value any) []string {
	switch typed := value.(type) {
	case []string:
		return uniqueNonEmptyStrings(typed)
	case []any:
		items := make([]string, 0, len(typed))
		for _, entry := range typed {
			text := strings.TrimSpace(artifactStringValue(entry))
			if text != "" {
				items = append(items, text)
			}
		}
		return uniqueNonEmptyStrings(items)
	default:
		return nil
	}
}

func sortedArtifactKeysFromDecoded(decoded map[string]map[string]any) []string {
	keys := make([]string, 0, len(decoded))
	for key := range decoded {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func artifactKeyForStep(keys []string, stepID, skillName string) string {
	prefix := "step:" + strings.TrimSpace(stepID) + ":"
	for _, key := range keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		candidate := strings.TrimPrefix(key, prefix)
		if skillruntime.IsAliasFor(candidate, skillName) {
			return key
		}
	}
	return ""
}

func selectPrimaryBySkills(decoded map[string]map[string]any, skillNames ...string) (string, map[string]any) {
	keys := sortedArtifactKeysFromDecoded(decoded)
	for _, skillName := range skillNames {
		for _, key := range keys {
			if artifactKeyMatchesSkillName(key, skillName) {
				return key, decoded[key]
			}
		}
	}
	return "", nil
}

func artifactKeyMatchesSkillName(key, skillName string) bool {
	index := strings.LastIndex(strings.TrimSpace(key), ":")
	if index < 0 || index == len(strings.TrimSpace(key))-1 {
		return false
	}
	return skillruntime.IsAliasFor(key[index+1:], skillName)
}

func uniqueNonEmptyStrings(items []string) []string {
	seen := map[string]struct{}{}
	result := make([]string, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func usageIntValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

func usageFloatValue(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case float32:
		return float64(typed)
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	default:
		return 0
	}
}

func usageRoundCost(value float64) float64 {
	return float64(int(value*100000000+0.5)) / 100000000
}

func artifactStringValue(value any) string {
	if value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return text
	}
	return fmt.Sprint(value)
}

func stringPointer(value string) *string {
	return &value
}
