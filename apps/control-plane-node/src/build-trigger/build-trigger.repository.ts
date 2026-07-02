import { Inject, Injectable } from '@nestjs/common';
import { Kysely, sql } from 'kysely';
import { DB } from '../db/db.module';

/** build job insert/갱신에 필요한 필드 (Go store.SaveDatasetBuildJob 부분집합). */
export interface NewBuildJob {
  job_id: string;
  project_id: string;
  dataset_id: string;
  dataset_version_id: string;
  build_type: string;
  status: string;
  request: Record<string, unknown>;
  triggered_by: string;
  created_at: string; // ISO
}

export interface ActiveBuildJobRow {
  job_id: string;
  build_type: string;
  status: string;
  created_at: string;
}

@Injectable()
export class BuildTriggerRepository {
  constructor(@Inject(DB) private readonly db: Kysely<any>) {}

  /** Go findActiveDatasetBuildJob — 같은 버전/타입의 queued|running 최신 1건 (idempotent 반환용). */
  async findActiveJob(
    projectId: string,
    datasetVersionId: string,
    buildType: string,
  ): Promise<ActiveBuildJobRow | undefined> {
    const result = await sql<ActiveBuildJobRow>`
      SELECT job_id::text AS job_id, build_type, status, created_at
      FROM dataset_build_jobs
      WHERE project_id = ${projectId}::uuid AND dataset_version_id = ${datasetVersionId}
        AND build_type = ${buildType} AND status IN ('queued', 'running')
      ORDER BY created_at DESC, job_id DESC
      LIMIT 1
    `.execute(this.db);
    return result.rows[0];
  }

  async insertJob(job: NewBuildJob): Promise<void> {
    await sql`
      INSERT INTO dataset_build_jobs (
        job_id, project_id, dataset_id, dataset_version_id, build_type, status,
        request, triggered_by, attempt, created_at
      ) VALUES (
        ${job.job_id}::uuid, ${job.project_id}::uuid, ${job.dataset_id}::uuid,
        ${job.dataset_version_id}, ${job.build_type}, ${job.status},
        ${JSON.stringify(job.request)}::jsonb, ${job.triggered_by}, 0, ${job.created_at}
      )
    `.execute(this.db);
  }

  /** dispatch 성공 — workflow_id 기록 (Go는 전체 row 재저장, 여기선 targeted UPDATE). */
  async setJobWorkflowId(jobId: string, workflowId: string): Promise<void> {
    await sql`
      UPDATE dataset_build_jobs SET workflow_id = ${workflowId}, error_message = NULL, last_error_type = NULL
      WHERE job_id = ${jobId}::uuid
    `.execute(this.db);
  }

  /** dispatch 실패 — Go dispatchDatasetBuildJob의 failed 마킹. */
  async markJobStartFailed(jobId: string, message: string): Promise<void> {
    await sql`
      UPDATE dataset_build_jobs
      SET status = 'failed', completed_at = NOW(),
          error_message = ${message}, last_error_type = 'workflow_start_failed'
      WHERE job_id = ${jobId}::uuid
    `.execute(this.db);
  }

  /**
   * build 접수 시 version 상태 큐잉 — Go는 GetDatasetVersion 결과 전체를 재저장하지만
   * 이 경로의 실질 변경은 상태 필드뿐이라 targeted UPDATE로 반영한다.
   * clean은 컬럼 + metadata 동시(clean_status 이중 저장), 나머지는 metadata만.
   */
  async setVersionBuildQueued(
    projectId: string,
    datasetVersionId: string,
    buildType: string,
  ): Promise<void> {
    const statusKey = `{${buildType}_status}`;
    if (buildType === 'clean') {
      await sql`
        UPDATE dataset_versions
        SET clean_status = 'queued',
            metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), ${statusKey}::text[], '"queued"')
        WHERE project_id = ${projectId}::uuid AND dataset_version_id = ${datasetVersionId}
      `.execute(this.db);
      return;
    }
    await sql`
      UPDATE dataset_versions
      SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), ${statusKey}::text[], '"queued"')
      WHERE project_id = ${projectId}::uuid AND dataset_version_id = ${datasetVersionId}
    `.execute(this.db);
  }
}
