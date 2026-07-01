import { Inject, Injectable } from '@nestjs/common';
import { Kysely, sql } from 'kysely';
import { DB } from '../db/db.module';

/** Postgres row 원형 (count는 bigint → pg가 string으로 반환). */
export interface ProjectCountsRow {
  project_id: string;
  name: string;
  description: string | null;
  created_at: Date;
  metadata: Record<string, unknown> | null;
  dataset_count: string;
  dataset_version_count: string;
  prompt_count: string;
  analysis_thread_count: string;
}

/**
 * Go ProjectService.withProjectCounts는 dataset를 돌며 version을 dataset별로 조회(N+1).
 * 여기선 상관 서브쿼리 단일 왕복으로 대체 — 계약(응답 값)은 동일, 쿼리 수만 개선.
 * dataset_version_count는 Go와 동일 의미(프로젝트 dataset에 속한 version 합산)로 계산.
 */
@Injectable()
export class ProjectsRepository {
  constructor(@Inject(DB) private readonly db: Kysely<any>) {}

  async listWithCounts(): Promise<ProjectCountsRow[]> {
    const result = await sql<ProjectCountsRow>`
      SELECT
        p.project_id::text AS project_id,
        p.name,
        p.description,
        p.created_at,
        p.metadata,
        (SELECT count(*) FROM datasets d WHERE d.project_id = p.project_id) AS dataset_count,
        (SELECT count(*) FROM dataset_versions v
           WHERE v.dataset_id IN (SELECT d.dataset_id FROM datasets d WHERE d.project_id = p.project_id)
        ) AS dataset_version_count,
        (SELECT count(*) FROM project_prompts pp WHERE pp.project_id = p.project_id) AS prompt_count,
        (SELECT count(*) FROM analysis_threads t WHERE t.project_id = p.project_id) AS analysis_thread_count
      FROM projects p
      ORDER BY p.created_at ASC, p.project_id ASC
    `.execute(this.db);
    return result.rows;
  }
}
