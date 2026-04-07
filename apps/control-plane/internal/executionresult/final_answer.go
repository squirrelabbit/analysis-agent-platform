package executionresult

import (
	"fmt"
	"strings"

	"analysis-support-platform/control-plane/internal/domain"
)

func BuildFinalAnswer(execution domain.ExecutionSummary) *domain.ExecutionFinalAnswer {
	if execution.FinalAnswerSnapshot != nil {
		answer := *execution.FinalAnswerSnapshot
		return &answer
	}
	if strings.TrimSpace(execution.Status) != "completed" {
		return nil
	}
	result := executionResultForPresentation(execution)
	answer := buildFallbackFinalAnswer(result, execution.Status)
	return &answer
}

func buildFallbackFinalAnswer(result domain.ExecutionResultV1, executionStatus string) domain.ExecutionFinalAnswer {
	headline := "분석 결과 요약"
	if result.PrimarySkillName != nil && strings.TrimSpace(*result.PrimarySkillName) != "" {
		headline = strings.TrimSpace(*result.PrimarySkillName) + " 결과 요약"
	}

	answerText := fmt.Sprintf("실행은 %s 상태입니다.", strings.TrimSpace(executionStatus))
	keyPoints := []string{}
	evidence := []map[string]any{}
	followUps := []string{}
	if result.Answer != nil {
		if summary := strings.TrimSpace(result.Answer.Summary); summary != "" {
			answerText = summary
		}
		keyPoints = limitStrings(uniqueNonEmptyStrings(result.Answer.KeyFindings), 5)
		evidence = limitEvidence(result.Answer.Evidence, 3)
		followUps = limitStrings(uniqueNonEmptyStrings(result.Answer.FollowUpQuestions), 5)
	}
	if answerText == "" {
		answerText = "실행 결과 요약을 생성하지 못했습니다."
	}

	return domain.ExecutionFinalAnswer{
		SchemaVersion:     "execution-final-answer-v1",
		Status:            "ready",
		GenerationMode:    "fallback",
		Headline:          headline,
		AnswerText:        answerText,
		KeyPoints:         keyPoints,
		Caveats:           limitStrings(uniqueNonEmptyStrings(result.Warnings), 4),
		Evidence:          evidence,
		FollowUpQuestions: followUps,
	}
}

func finalAnswerPreview(answer *domain.ExecutionFinalAnswer) string {
	if answer == nil {
		return ""
	}
	if text := strings.TrimSpace(answer.AnswerText); text != "" {
		return text
	}
	return strings.TrimSpace(answer.Headline)
}
