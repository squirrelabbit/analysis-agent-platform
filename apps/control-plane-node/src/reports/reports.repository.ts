import { Inject, Injectable } from '@nestjs/common';
import { Kysely, sql } from 'kysely';
import { DB } from '../db/db.module';

export interface ReportSummaryRow {
  report_id: string;
  project_id: string;
  title: string;
  block_count: number;
  created_at: string;
  updated_at: string;
}

export interface ReportRow {
  report_id: string;
  project_id: string;
  title: string;
  blocks_json: unknown;
  dataset_version_id: string;
  created_at: string;
  updated_at: string;
}

@Injectable()
export class ReportsRepository {
  constructor(@Inject(DB) private readonly db: Kysely<any>) {}

  /** Go requireProject — 404 project 판정용. */
  async projectExists(projectId: string): Promise<boolean> {
    const result = await sql`
      SELECT 1 FROM projects WHERE project_id = ${projectId}::uuid
    `.execute(this.db);
    return result.rows.length > 0;
  }

  async list(projectId: string): Promise<ReportSummaryRow[]> {
    const result = await sql<ReportSummaryRow>`
      SELECT report_id, project_id::text AS project_id, title,
             COALESCE(jsonb_array_length(blocks_json), 0)::int AS block_count,
             created_at, updated_at
      FROM reports
      WHERE project_id = ${projectId}::uuid
      ORDER BY updated_at DESC, report_id DESC
    `.execute(this.db);
    return result.rows;
  }

  async get(projectId: string, reportId: string): Promise<ReportRow | undefined> {
    const result = await sql<ReportRow>`
      SELECT report_id, project_id::text AS project_id, title, blocks_json,
             COALESCE(dataset_version_id, '') AS dataset_version_id, created_at, updated_at
      FROM reports
      WHERE project_id = ${projectId}::uuid AND report_id = ${reportId}
    `.execute(this.db);
    return result.rows[0];
  }
}
