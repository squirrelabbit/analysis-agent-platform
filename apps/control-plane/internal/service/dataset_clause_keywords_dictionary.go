package service

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/obs"
	"analysis-support-platform/control-plane/internal/store"
)

// 키워드 정제 사전 (silverone 2026-06-25, ADR 후보).
//
// 운영자가 추출된 키워드 결과를 정제하는 dataset 단위 사전. 원본 artifact는
// 불변이고, 조회 시 overlay로 적용된다(Phase 1). 규칙 2종:
//   - block:   source_term을 키워드 결과에서 제외(불용어 추가)
//   - synonym: source_term을 target_term(대표어)로 병합(빈도/랭크/대표문장 합산)
//
// 현재 상태는 keyword_dictionary_rules(soft delete=active), 변경 이력은
// append-only keyword_dictionary_events에 누적한다("왜 사라졌나/합쳐졌나" 감사).

const (
	keywordRuleBlock   = "block"
	keywordRuleSynonym = "synonym"

	keywordEventAdd        = "add"
	keywordEventUpdate     = "update"
	keywordEventDeactivate = "deactivate"
	keywordEventReactivate = "reactivate"
	keywordEventDelete     = "delete"

	keywordDefaultReason = "운영자 키워드 정제"
)

// ListKeywordDictionaryRules — dataset 사전 규칙 목록. activeOnly면 활성만.
func (s *DatasetService) ListKeywordDictionaryRules(projectID, datasetID string, activeOnly bool) ([]domain.KeywordDictionaryRule, error) {
	if _, err := s.GetDataset(projectID, datasetID); err != nil {
		return nil, err
	}
	return s.store.ListKeywordDictionaryRules(projectID, datasetID, activeOnly)
}

// ListKeywordDictionaryEvents — 사전 변경 이력(최신순).
func (s *DatasetService) ListKeywordDictionaryEvents(projectID, datasetID string) ([]domain.KeywordDictionaryEvent, error) {
	if _, err := s.GetDataset(projectID, datasetID); err != nil {
		return nil, err
	}
	return s.store.ListKeywordDictionaryEvents(projectID, datasetID)
}

// SetKeywordDictionaryRule — 규칙 생성 또는 갱신(같은 source_term 활성 규칙이 있으면
// 업데이트). 검증 후 upsert + event append. actorID는 세션 사용자(없으면 빈 값).
func (s *DatasetService) SetKeywordDictionaryRule(
	projectID, datasetID string,
	req domain.KeywordDictionaryRuleRequest,
	actorID string,
) (domain.KeywordDictionaryRule, error) {
	if _, err := s.GetDataset(projectID, datasetID); err != nil {
		return domain.KeywordDictionaryRule{}, err
	}

	ruleType := strings.TrimSpace(req.RuleType)
	source := strings.TrimSpace(req.SourceTerm)
	target := strings.TrimSpace(req.TargetTerm)
	if ruleType != keywordRuleBlock && ruleType != keywordRuleSynonym {
		return domain.KeywordDictionaryRule{}, ErrInvalidArgument{Message: "rule_type must be 'block' or 'synonym'"}
	}
	if source == "" {
		return domain.KeywordDictionaryRule{}, ErrInvalidArgument{Message: "source_term is required"}
	}
	if ruleType == keywordRuleBlock {
		target = ""
	} else {
		if target == "" {
			return domain.KeywordDictionaryRule{}, ErrInvalidArgument{Message: "target_term is required for synonym rule"}
		}
		if target == source {
			return domain.KeywordDictionaryRule{}, ErrInvalidArgument{Message: "target_term must differ from source_term"}
		}
	}

	active, err := s.store.ListKeywordDictionaryRules(projectID, datasetID, true)
	if err != nil {
		return domain.KeywordDictionaryRule{}, err
	}
	// 검증: 사전 일관성 (이 규칙을 적용했을 때 충돌이 없어야).
	if verr := validateKeywordRuleAgainst(ruleType, source, target, active); verr != nil {
		return domain.KeywordDictionaryRule{}, verr
	}

	now := time.Now().UTC()
	reason := strings.TrimSpace(req.Reason)
	if reason == "" {
		reason = keywordDefaultReason
	}

	// 같은 source_term 활성 규칙이 있으면 그 id를 재사용(업데이트), 없으면 신규.
	var existing *domain.KeywordDictionaryRule
	for i := range active {
		if active[i].SourceTerm == source {
			existing = &active[i]
			break
		}
	}
	rule := domain.KeywordDictionaryRule{
		ProjectID:  projectID,
		DatasetID:  datasetID,
		RuleType:   ruleType,
		SourceTerm: source,
		TargetTerm: target,
		Active:     true,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	eventType := keywordEventAdd
	var before string
	if existing != nil {
		rule.ID = existing.ID
		rule.CreatedAt = existing.CreatedAt
		eventType = keywordEventUpdate
		before = keywordRulePayload(*existing)
	} else {
		rule.ID = id.New()
	}

	if err := s.store.UpsertKeywordDictionaryRule(rule); err != nil {
		return domain.KeywordDictionaryRule{}, err
	}
	s.appendKeywordEvent(projectID, datasetID, rule.ID, eventType, before, keywordRulePayload(rule), reason, actorID, now)
	return rule, nil
}

// SetKeywordDictionaryRuleActive — 규칙 비활성(삭제)/재활성(복구). soft delete라
// row는 보존되고 active=false만 된다. event로 이력 기록.
func (s *DatasetService) SetKeywordDictionaryRuleActive(
	projectID, datasetID, ruleID string,
	active bool,
	reason, actorID string,
) (domain.KeywordDictionaryRule, error) {
	if _, err := s.GetDataset(projectID, datasetID); err != nil {
		return domain.KeywordDictionaryRule{}, err
	}
	current, err := s.store.GetKeywordDictionaryRule(projectID, datasetID, ruleID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.KeywordDictionaryRule{}, ErrNotFound{Resource: "keyword dictionary rule"}
		}
		return domain.KeywordDictionaryRule{}, err
	}
	if current.Active == active {
		return current, nil // no-op (이미 그 상태)
	}
	// 재활성 시 같은 source_term의 다른 활성 규칙과 충돌하면 거부.
	if active {
		others, lerr := s.store.ListKeywordDictionaryRules(projectID, datasetID, true)
		if lerr != nil {
			return domain.KeywordDictionaryRule{}, lerr
		}
		for _, o := range others {
			if o.ID != ruleID && o.SourceTerm == current.SourceTerm {
				return domain.KeywordDictionaryRule{}, ErrInvalidArgument{Message: "another active rule already exists for this source_term"}
			}
		}
	}
	now := time.Now().UTC()
	if err := s.store.SetKeywordDictionaryRuleActive(projectID, datasetID, ruleID, active, now); err != nil {
		if err == store.ErrNotFound {
			return domain.KeywordDictionaryRule{}, ErrNotFound{Resource: "keyword dictionary rule"}
		}
		return domain.KeywordDictionaryRule{}, err
	}
	updated := current
	updated.Active = active
	updated.UpdatedAt = now
	eventType := keywordEventDeactivate
	if active {
		eventType = keywordEventReactivate
	}
	r := strings.TrimSpace(reason)
	if r == "" {
		r = keywordDefaultReason
	}
	s.appendKeywordEvent(projectID, datasetID, ruleID, eventType, keywordRulePayload(current), keywordRulePayload(updated), r, actorID, now)
	return updated, nil
}

// DeleteKeywordDictionaryRule — 규칙을 목록에서 완전 제거(hard delete). 해제
// (active=false)와 달리 행을 지운다. 변경 이력 event는 append-only로 남아(rule_id
// 참조만, FK 아님) "삭제됨"이 감사에 보존된다. 되돌리려면 다시 규칙을 추가.
func (s *DatasetService) DeleteKeywordDictionaryRule(
	projectID, datasetID, ruleID string,
	reason, actorID string,
) error {
	if _, err := s.GetDataset(projectID, datasetID); err != nil {
		return err
	}
	current, err := s.store.GetKeywordDictionaryRule(projectID, datasetID, ruleID)
	if err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "keyword dictionary rule"}
		}
		return err
	}
	if err := s.store.DeleteKeywordDictionaryRule(projectID, datasetID, ruleID); err != nil {
		if err == store.ErrNotFound {
			return ErrNotFound{Resource: "keyword dictionary rule"}
		}
		return err
	}
	r := strings.TrimSpace(reason)
	if r == "" {
		r = keywordDefaultReason
	}
	s.appendKeywordEvent(projectID, datasetID, ruleID, keywordEventDelete, keywordRulePayload(current), "", r, actorID, time.Now().UTC())
	return nil
}

func (s *DatasetService) appendKeywordEvent(projectID, datasetID, ruleID, eventType, before, after, reason, actorID string, at time.Time) {
	// 이력 적재 실패는 본 작업을 막지 않되 obs로 가시화(silent fallback 금지).
	if err := s.store.AppendKeywordDictionaryEvent(domain.KeywordDictionaryEvent{
		ID:            id.New(),
		ProjectID:     projectID,
		DatasetID:     datasetID,
		RuleID:        ruleID,
		EventType:     eventType,
		BeforePayload: before,
		AfterPayload:  after,
		Reason:        reason,
		ActorID:       actorID,
		CreatedAt:     at,
	}); err != nil {
		obs.Logger.Warn("keyword_dictionary.event_append_failed",
			"error", err.Error(), "rule_id", ruleID, "event_type", eventType)
	}
}

// validateKeywordRuleAgainst — 새/갱신 규칙이 활성 사전과 일관적인지 검증.
//   - synonym target은 blocklist에 없어야(병합 대상이 제외되면 모순)
//   - synonym target은 다른 활성 synonym의 source가 아니어야(체인 금지, 1-level)
//   - synonym source는 다른 활성 synonym의 target이 아니어야(대표어를 다시 병합 금지)
func validateKeywordRuleAgainst(ruleType, source, target string, active []domain.KeywordDictionaryRule) error {
	blocked := map[string]bool{}
	synSources := map[string]bool{}
	synTargets := map[string]bool{}
	for _, r := range active {
		if r.SourceTerm == source { // 같은 source는 업데이트라 자기 자신 제외
			continue
		}
		switch r.RuleType {
		case keywordRuleBlock:
			blocked[r.SourceTerm] = true
		case keywordRuleSynonym:
			synSources[r.SourceTerm] = true
			synTargets[r.TargetTerm] = true
		}
	}
	if ruleType == keywordRuleSynonym {
		if blocked[target] {
			return ErrInvalidArgument{Message: fmt.Sprintf("target_term '%s' is in blocklist", target)}
		}
		if synSources[target] {
			return ErrInvalidArgument{Message: fmt.Sprintf("target_term '%s' is itself merged into another term (chain not allowed)", target)}
		}
		if synTargets[source] {
			return ErrInvalidArgument{Message: fmt.Sprintf("source_term '%s' is a canonical target of another rule (cannot re-merge a representative)", source)}
		}
	}
	return nil
}

func keywordRulePayload(r domain.KeywordDictionaryRule) string {
	b, err := json.Marshal(map[string]any{
		"rule_type":   r.RuleType,
		"source_term": r.SourceTerm,
		"target_term": r.TargetTerm,
		"active":      r.Active,
	})
	if err != nil {
		return ""
	}
	return string(b)
}

// buildKeywordDictionarySource — clause_keywords artifact를 읽는 DuckDB source
// 표현식을 만든다. 활성 규칙이 없으면 평범한 read_json. 있으면 서브쿼리로 감싸
// block(WHERE NOT IN)·synonym(* REPLACE(CASE … AS keyword)) 을 적용한다.
// 모든 하위 집계가 이 source 위에서 돌므로 빈도/랭크/대표문장 재집계가 자동.
func buildKeywordDictionarySource(ref string, rules []domain.KeywordDictionaryRule) string {
	base := fmt.Sprintf("read_json('%s', format='newline_delimited')", escapeDuckDBLiteral(ref))
	if len(rules) == 0 {
		return base
	}
	var blocked []string
	synByTarget := map[string][]string{}
	for _, r := range rules {
		if !r.Active {
			continue
		}
		switch r.RuleType {
		case keywordRuleBlock:
			blocked = append(blocked, r.SourceTerm)
		case keywordRuleSynonym:
			synByTarget[r.TargetTerm] = append(synByTarget[r.TargetTerm], r.SourceTerm)
		}
	}
	if len(blocked) == 0 && len(synByTarget) == 0 {
		return base
	}

	// synonym: CASE WHEN keyword IN (...) THEN 'target' … ELSE keyword END AS keyword
	selectClause := "*"
	if len(synByTarget) > 0 {
		var whens []string
		for target, sources := range synByTarget {
			whens = append(whens, fmt.Sprintf("WHEN keyword IN (%s) THEN '%s'",
				quoteTermList(sources), escapeDuckDBLiteral(target)))
		}
		selectClause = fmt.Sprintf("* REPLACE (CASE %s ELSE keyword END AS keyword)",
			strings.Join(whens, " "))
	}

	where := ""
	if len(blocked) > 0 {
		where = fmt.Sprintf(" WHERE keyword IS NULL OR keyword NOT IN (%s)", quoteTermList(blocked))
	}

	return fmt.Sprintf("(SELECT %s FROM %s%s)", selectClause, base, where)
}

// countActiveKeywordRules — view summary 노출용 활성 규칙 수.
func countActiveKeywordRules(rules []domain.KeywordDictionaryRule) int {
	n := 0
	for _, r := range rules {
		if r.Active {
			n++
		}
	}
	return n
}

// quoteTermList — DuckDB IN 절용 '...','...' (escape 포함).
func quoteTermList(terms []string) string {
	parts := make([]string, 0, len(terms))
	for _, t := range terms {
		parts = append(parts, "'"+escapeDuckDBLiteral(t)+"'")
	}
	return strings.Join(parts, ", ")
}
