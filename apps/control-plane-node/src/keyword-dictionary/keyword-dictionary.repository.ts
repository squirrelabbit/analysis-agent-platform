import { Inject, Injectable } from '@nestjs/common';
import { Kysely, sql } from 'kysely';
import { DB } from '../db/db.module';

/** keyword_dictionary_rules row — Go store.ListKeywordDictionaryRules SELECT 대응. */
export interface KeywordDictionaryRuleRow {
  id: string;
  project_id: string;
  dataset_id: string;
  rule_type: string;
  source_term: string;
  target_term: string;
  active: boolean;
  created_at: string;
  updated_at: string;
}

/** keyword_dictionary_events row — Go store.ListKeywordDictionaryEvents SELECT 대응. */
export interface KeywordDictionaryEventRow {
  id: string;
  project_id: string;
  dataset_id: string;
  rule_id: string;
  event_type: string;
  before_payload: string;
  after_payload: string;
  reason: string;
  actor_id: string;
  created_at: string;
}

@Injectable()
export class KeywordDictionaryRepository {
  constructor(@Inject(DB) private readonly db: Kysely<any>) {}

  /** Go 서비스 진입부의 GetDataset 존재 확인(404 dataset) 대응. */
  async datasetExists(projectId: string, datasetId: string): Promise<boolean> {
    const result = await sql`
      SELECT 1 FROM datasets
      WHERE project_id = ${projectId}::uuid AND dataset_id = ${datasetId}::uuid
    `.execute(this.db);
    return result.rows.length > 0;
  }

  async listRules(
    projectId: string,
    datasetId: string,
    activeOnly: boolean,
  ): Promise<KeywordDictionaryRuleRow[]> {
    const activeFilter = activeOnly ? sql`AND active` : sql``;
    const result = await sql<KeywordDictionaryRuleRow>`
      SELECT id, project_id::text AS project_id, dataset_id::text AS dataset_id, rule_type,
             source_term, COALESCE(target_term, '') AS target_term, active, created_at, updated_at
      FROM keyword_dictionary_rules
      WHERE project_id = ${projectId}::uuid AND dataset_id = ${datasetId}::uuid
      ${activeFilter}
      ORDER BY rule_type, source_term
    `.execute(this.db);
    return result.rows;
  }

  async listEvents(projectId: string, datasetId: string): Promise<KeywordDictionaryEventRow[]> {
    const result = await sql<KeywordDictionaryEventRow>`
      SELECT id, project_id::text AS project_id, dataset_id::text AS dataset_id, rule_id, event_type,
             COALESCE(before_payload, '') AS before_payload, COALESCE(after_payload, '') AS after_payload,
             COALESCE(reason, '') AS reason, COALESCE(actor_id, '') AS actor_id, created_at
      FROM keyword_dictionary_events
      WHERE project_id = ${projectId}::uuid AND dataset_id = ${datasetId}::uuid
      ORDER BY created_at DESC, id
    `.execute(this.db);
    return result.rows;
  }
}
