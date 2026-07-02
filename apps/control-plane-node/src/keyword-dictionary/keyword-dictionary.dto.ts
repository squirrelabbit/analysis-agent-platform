/**
 * 키워드 정제 사전 read 계약 — Go domain.KeywordDictionaryRule / KeywordDictionaryEvent.
 * target_term은 ''면 생략(Go omitempty), event의 payload/reason/actor_id도 동일.
 */

export interface KeywordDictionaryRuleDto {
  id: string;
  project_id: string;
  dataset_id: string;
  rule_type: string;
  source_term: string;
  target_term?: string;
  active: boolean;
  created_at: string;
  updated_at: string;
}

export interface KeywordDictionaryRuleListResponse {
  items: KeywordDictionaryRuleDto[];
}

export interface KeywordDictionaryEventDto {
  id: string;
  project_id: string;
  dataset_id: string;
  rule_id: string;
  event_type: string;
  before_payload?: string;
  after_payload?: string;
  reason?: string;
  actor_id?: string;
  created_at: string;
}

export interface KeywordDictionaryEventListResponse {
  items: KeywordDictionaryEventDto[];
}
