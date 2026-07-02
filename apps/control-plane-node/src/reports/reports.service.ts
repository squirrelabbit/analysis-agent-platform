import { Injectable } from '@nestjs/common';
import { notFound } from '../common/errors';
import { goTimestamptz } from '../common/go-time';
import { ReportsRepository } from './reports.repository';

/**
 * 보고서 read — Go ListReports/GetReport 계약 동일. blocks는 opaque JSON 배열
 * (contract는 프론트 보고서 에디터 소유, control-plane은 영속만).
 */
@Injectable()
export class ReportsService {
  constructor(private readonly repo: ReportsRepository) {}

  async list(projectId: string): Promise<{ items: Record<string, unknown>[] }> {
    if (!(await this.repo.projectExists(projectId))) {
      throw notFound('project');
    }
    const rows = await this.repo.list(projectId);
    return {
      items: rows.map((row) => ({
        report_id: row.report_id,
        project_id: row.project_id,
        title: row.title,
        block_count: row.block_count,
        created_at: goTimestamptz(row.created_at),
        updated_at: goTimestamptz(row.updated_at),
      })),
    };
  }

  /** Go GetReport은 project 존재를 따로 확인하지 않는다 — 조합 miss = 404 report. */
  async get(projectId: string, reportId: string): Promise<Record<string, unknown>> {
    const row = await this.repo.get(projectId, reportId.trim());
    if (row === undefined) {
      throw notFound('report');
    }
    const dto: Record<string, unknown> = {
      report_id: row.report_id,
      project_id: row.project_id,
      title: row.title,
    };
    if (row.dataset_version_id !== '') {
      dto['dataset_version_id'] = row.dataset_version_id;
    }
    dto['blocks'] = row.blocks_json ?? [];
    dto['created_at'] = goTimestamptz(row.created_at);
    dto['updated_at'] = goTimestamptz(row.updated_at);
    return dto;
  }
}
