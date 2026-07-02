import { Injectable } from '@nestjs/common';
import { notFound } from '../common/errors';
import { goTimestamptz } from '../common/go-time';
import {
  KeywordDictionaryEventDto,
  KeywordDictionaryEventListResponse,
  KeywordDictionaryRuleDto,
  KeywordDictionaryRuleListResponse,
} from './keyword-dictionary.dto';
import {
  KeywordDictionaryEventRow,
  KeywordDictionaryRepository,
  KeywordDictionaryRuleRow,
} from './keyword-dictionary.repository';

@Injectable()
export class KeywordDictionaryService {
  constructor(private readonly repo: KeywordDictionaryRepository) {}

  /** Go ListKeywordDictionaryRules — dataset 미존재면 404. */
  async listRules(
    projectId: string,
    datasetId: string,
    activeOnly: boolean,
  ): Promise<KeywordDictionaryRuleListResponse> {
    if (!(await this.repo.datasetExists(projectId, datasetId))) {
      throw notFound('dataset');
    }
    const rows = await this.repo.listRules(projectId, datasetId, activeOnly);
    return { items: rows.map(toRuleDto) };
  }

  /** Go ListKeywordDictionaryEvents — 변경 이력(최신순). */
  async listEvents(
    projectId: string,
    datasetId: string,
  ): Promise<KeywordDictionaryEventListResponse> {
    if (!(await this.repo.datasetExists(projectId, datasetId))) {
      throw notFound('dataset');
    }
    const rows = await this.repo.listEvents(projectId, datasetId);
    return { items: rows.map(toEventDto) };
  }
}

function toRuleDto(row: KeywordDictionaryRuleRow): KeywordDictionaryRuleDto {
  const dto: KeywordDictionaryRuleDto = {
    id: row.id,
    project_id: row.project_id,
    dataset_id: row.dataset_id,
    rule_type: row.rule_type,
    source_term: row.source_term,
    active: row.active,
    created_at: goTimestamptz(row.created_at),
    updated_at: goTimestamptz(row.updated_at),
  };
  if (row.target_term !== '') {
    dto.target_term = row.target_term;
  }
  return dto;
}

function toEventDto(row: KeywordDictionaryEventRow): KeywordDictionaryEventDto {
  const dto: KeywordDictionaryEventDto = {
    id: row.id,
    project_id: row.project_id,
    dataset_id: row.dataset_id,
    rule_id: row.rule_id,
    event_type: row.event_type,
    created_at: goTimestamptz(row.created_at),
  };
  if (row.before_payload !== '') {
    dto.before_payload = row.before_payload;
  }
  if (row.after_payload !== '') {
    dto.after_payload = row.after_payload;
  }
  if (row.reason !== '') {
    dto.reason = row.reason;
  }
  if (row.actor_id !== '') {
    dto.actor_id = row.actor_id;
  }
  return dto;
}
