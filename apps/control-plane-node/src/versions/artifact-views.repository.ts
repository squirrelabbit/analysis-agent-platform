import { Inject, Injectable } from '@nestjs/common';
import { Kysely, sql } from 'kysely';
import { DB } from '../db/db.module';

/** build job 최신 1건 — view status/enrich에 필요한 필드만. */
export interface LatestBuildJobRow {
  job_id: string;
  status: string;
  request: Record<string, unknown> | null;
  started_at: string | null;
  completed_at: string | null;
  error_message: string | null;
}

/** doc_genuineness override overlay에 필요한 필드 (Go ListDocGenuinenessOverrides). */
export interface DocGenuinenessOverrideRow {
  doc_id: string;
  original_genuineness: string;
  original_reason: string;
  override_genuineness: string;
  override_reason: string;
}

/** clause_label override overlay에 필요한 필드 (Go ListClauseLabelOverrides). */
export interface ClauseLabelOverrideRow {
  clause_id: string;
  original_aspect: string;
  original_sentiment: string;
  override_aspect: string;
  override_sentiment: string;
  override_reason: string;
}

@Injectable()
export class ArtifactViewsRepository {
  constructor(@Inject(DB) private readonly db: Kysely<any>) {}

  /** Go latestJobForBuildType — created_at DESC(동률 job_id DESC) 첫 매칭 1건. */
  async latestJob(
    projectId: string,
    datasetVersionId: string,
    buildType: string,
  ): Promise<LatestBuildJobRow | undefined> {
    const result = await sql<LatestBuildJobRow>`
      SELECT job_id::text AS job_id, status, request, started_at, completed_at, error_message
      FROM dataset_build_jobs
      WHERE project_id = ${projectId}::uuid AND dataset_version_id = ${datasetVersionId}
        AND build_type = ${buildType}
      ORDER BY created_at DESC, job_id DESC
      LIMIT 1
    `.execute(this.db);
    return result.rows[0];
  }

  async listDocGenuinenessOverrides(
    projectId: string,
    datasetVersionId: string,
  ): Promise<DocGenuinenessOverrideRow[]> {
    const result = await sql<DocGenuinenessOverrideRow>`
      SELECT doc_id,
             COALESCE(original_genuineness, '') AS original_genuineness,
             COALESCE(original_reason, '') AS original_reason,
             override_genuineness,
             COALESCE(override_reason, '') AS override_reason
      FROM doc_genuineness_overrides
      WHERE project_id = ${projectId}::uuid AND dataset_version_id = ${datasetVersionId}
      ORDER BY doc_id
    `.execute(this.db);
    return result.rows;
  }

  async listClauseLabelOverrides(
    projectId: string,
    datasetVersionId: string,
  ): Promise<ClauseLabelOverrideRow[]> {
    const result = await sql<ClauseLabelOverrideRow>`
      SELECT clause_id,
             COALESCE(original_aspect, '') AS original_aspect,
             COALESCE(original_sentiment, '') AS original_sentiment,
             COALESCE(override_aspect, '') AS override_aspect,
             COALESCE(override_sentiment, '') AS override_sentiment,
             COALESCE(override_reason, '') AS override_reason
      FROM clause_label_overrides
      WHERE project_id = ${projectId}::uuid AND dataset_version_id = ${datasetVersionId}
      ORDER BY clause_id
    `.execute(this.db);
    return result.rows;
  }
}
