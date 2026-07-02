import { Inject, Injectable } from '@nestjs/common';
import { Kysely, sql } from 'kysely';
import { DB } from '../db/db.module';

/** dataset_versions row 원형 — 목록/상세 응답이 쓰는 컬럼만 (Go store SELECT의 부분집합). */
export interface DatasetVersionRow {
  dataset_version_id: string;
  dataset_id: string;
  storage_uri: string;
  data_type: string;
  metadata: Record<string, unknown> | null;
  clean_status: string | null;
  cleaned_at: string | null;
  created_at: string;
  ready_at: string | null;
}

/** 활성 버전 판정 + view 합성(추천 제외어)에 필요한 dataset 조각. */
export interface DatasetActiveRow {
  dataset_id: string;
  active_dataset_version_id: string | null;
  metadata: Record<string, unknown> | null;
}

const VERSION_COLUMNS = sql`
  dataset_version_id, dataset_id::text AS dataset_id, storage_uri, data_type,
  metadata, clean_status, cleaned_at, created_at, ready_at
`;

@Injectable()
export class VersionsRepository {
  constructor(@Inject(DB) private readonly db: Kysely<any>) {}

  /** Go GetDatasetVersion/ListDatasetVersions는 진입 전에 GetDataset로 404 dataset 판정. */
  async getDataset(projectId: string, datasetId: string): Promise<DatasetActiveRow | undefined> {
    const result = await sql<DatasetActiveRow>`
      SELECT dataset_id::text AS dataset_id, active_dataset_version_id, metadata
      FROM datasets
      WHERE project_id = ${projectId}::uuid AND dataset_id = ${datasetId}::uuid
    `.execute(this.db);
    return result.rows[0];
  }

  async list(projectId: string, datasetId: string): Promise<DatasetVersionRow[]> {
    const result = await sql<DatasetVersionRow>`
      SELECT ${VERSION_COLUMNS}
      FROM dataset_versions
      WHERE project_id = ${projectId}::uuid AND dataset_id = ${datasetId}::uuid
      ORDER BY created_at DESC, dataset_version_id DESC
    `.execute(this.db);
    return result.rows;
  }

  async get(projectId: string, datasetVersionId: string): Promise<DatasetVersionRow | undefined> {
    const result = await sql<DatasetVersionRow>`
      SELECT ${VERSION_COLUMNS}
      FROM dataset_versions
      WHERE project_id = ${projectId}::uuid AND dataset_version_id = ${datasetVersionId}
    `.execute(this.db);
    return result.rows[0];
  }
}
