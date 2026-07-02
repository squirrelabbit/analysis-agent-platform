import { Injectable } from '@nestjs/common';
import { notFound } from '../common/errors';
import { goTimestamptz } from '../common/go-time';
import { ProjectDto, ProjectListResponse } from './project.dto';
import { ProjectCountsRow, ProjectsRepository } from './projects.repository';

@Injectable()
export class ProjectsService {
  constructor(private readonly repo: ProjectsRepository) {}

  async list(): Promise<ProjectListResponse> {
    const rows = await this.repo.listWithCounts();
    return { items: rows.map((r) => this.toDto(r)) };
  }

  async get(projectId: string): Promise<ProjectDto> {
    const row = await this.repo.getWithCounts(projectId);
    if (row === undefined) {
      throw notFound('project');
    }
    return this.toDto(row);
  }

  private toDto(row: ProjectCountsRow): ProjectDto {
    const dto: ProjectDto = {
      project_id: row.project_id,
      name: row.name,
      created_at: goTimestamptz(row.created_at),
      dataset_count: Number(row.dataset_count),
      dataset_version_count: Number(row.dataset_version_count),
      scenario_count: 0,
      prompt_count: Number(row.prompt_count),
      analysis_thread_count: Number(row.analysis_thread_count),
    };
    // Go: *string omitempty — null/빈값이면 키 생략.
    if (row.description != null && row.description !== '') {
      dto.description = row.description;
    }
    // Go: metadata map omitempty — null/빈 객체면 키 생략(gunbam처럼).
    if (row.metadata != null && Object.keys(row.metadata).length > 0) {
      dto.metadata = row.metadata;
    }
    return dto;
  }
}
