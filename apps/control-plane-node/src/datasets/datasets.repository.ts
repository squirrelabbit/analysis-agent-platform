import { Inject, Injectable } from '@nestjs/common';
import { Kysely, sql } from 'kysely';
import { DB } from '../db/db.module';

/** datasets row 원형 — Go store.GetDataset/ListDatasets SELECT과 동일 컬럼. */
export interface DatasetRow {
  dataset_id: string;
  project_id: string;
  name: string;
  description: string | null;
  data_type: string;
  active_dataset_version_id: string | null;
  active_version_updated_at: string | null;
  metadata: Record<string, unknown> | null;
  created_at: string;
}

@Injectable()
export class DatasetsRepository {
  constructor(@Inject(DB) private readonly db: Kysely<any>) {}

  /** Go ListDatasets는 진입 전에 store.GetProject로 프로젝트 존재를 확인한다(404 project). */
  async projectExists(projectId: string): Promise<boolean> {
    const result = await sql`
      SELECT 1 FROM projects WHERE project_id = ${projectId}::uuid
    `.execute(this.db);
    return result.rows.length > 0;
  }

  async list(projectId: string): Promise<DatasetRow[]> {
    const result = await sql<DatasetRow>`
      SELECT dataset_id::text AS dataset_id, project_id::text AS project_id, name, description,
             data_type, active_dataset_version_id, active_version_updated_at, metadata, created_at
      FROM datasets
      WHERE project_id = ${projectId}::uuid
      ORDER BY created_at ASC, dataset_id ASC
    `.execute(this.db);
    return result.rows;
  }

  async get(projectId: string, datasetId: string): Promise<DatasetRow | undefined> {
    const result = await sql<DatasetRow>`
      SELECT dataset_id::text AS dataset_id, project_id::text AS project_id, name, description,
             data_type, active_dataset_version_id, active_version_updated_at, metadata, created_at
      FROM datasets
      WHERE project_id = ${projectId}::uuid AND dataset_id = ${datasetId}::uuid
    `.execute(this.db);
    return result.rows[0];
  }
}
