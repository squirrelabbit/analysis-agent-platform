import { Inject, Injectable } from '@nestjs/common';
import { Kysely, sql } from 'kysely';
import { DB } from '../db/db.module';

/** dataset_build_jobs row 원형 — Go store.GetDatasetBuildJob SELECT과 동일 컬럼. */
export interface BuildJobRow {
  job_id: string;
  project_id: string;
  dataset_id: string;
  dataset_version_id: string;
  build_type: string;
  status: string;
  request: Record<string, unknown> | null;
  triggered_by: string | null;
  workflow_id: string | null;
  workflow_run_id: string | null;
  attempt: number;
  error_message: string | null;
  last_error_type: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

@Injectable()
export class BuildJobsRepository {
  constructor(@Inject(DB) private readonly db: Kysely<any>) {}

  async get(projectId: string, jobId: string): Promise<BuildJobRow | undefined> {
    const result = await sql<BuildJobRow>`
      SELECT job_id::text AS job_id, project_id::text AS project_id, dataset_id::text AS dataset_id,
             dataset_version_id, build_type, status, request, triggered_by, workflow_id,
             workflow_run_id, attempt, error_message, last_error_type,
             created_at, started_at, completed_at
      FROM dataset_build_jobs
      WHERE project_id = ${projectId}::uuid AND job_id = ${jobId}::uuid
    `.execute(this.db);
    return result.rows[0];
  }

  /**
   * job diagnostics enrich용 version metadata만 조회 — Go는 GetDatasetVersion 전체를
   * 읽지만 enrich가 쓰는 건 metadata뿐이다. 미존재면 undefined (Go도 enrich skip).
   */
  async getVersionMetadata(
    projectId: string,
    datasetVersionId: string,
  ): Promise<Record<string, unknown> | null | undefined> {
    const result = await sql<{ metadata: Record<string, unknown> | null }>`
      SELECT metadata FROM dataset_versions
      WHERE project_id = ${projectId}::uuid AND dataset_version_id = ${datasetVersionId}
    `.execute(this.db);
    if (result.rows.length === 0) {
      return undefined;
    }
    return result.rows[0].metadata;
  }
}
