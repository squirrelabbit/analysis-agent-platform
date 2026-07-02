import { Injectable } from '@nestjs/common';
import { notFound } from '../common/errors';
import { goTimestamptz } from '../common/go-time';
import { DatasetDto, DatasetListResponse } from './dataset.dto';
import { DatasetRow, DatasetsRepository } from './datasets.repository';

@Injectable()
export class DatasetsService {
  constructor(private readonly repo: DatasetsRepository) {}

  async list(projectId: string): Promise<DatasetListResponse> {
    // Go DatasetService.ListDatasets — 프로젝트 미존재면 404 "project not found".
    if (!(await this.repo.projectExists(projectId))) {
      throw notFound('project');
    }
    const rows = await this.repo.list(projectId);
    return { items: rows.map((r) => this.toDto(r)) };
  }

  async get(projectId: string, datasetId: string): Promise<DatasetDto> {
    // Go GetDataset은 프로젝트 존재를 따로 확인하지 않는다 — 조합 miss = 404 dataset.
    const row = await this.repo.get(projectId, datasetId);
    if (row === undefined) {
      throw notFound('dataset');
    }
    return this.toDto(row);
  }

  private toDto(row: DatasetRow): DatasetDto {
    const dto: DatasetDto = {
      dataset_id: row.dataset_id,
      project_id: row.project_id,
      name: row.name,
      data_type: row.data_type,
      created_at: goTimestamptz(row.created_at),
    };
    if (row.description != null) {
      dto.description = row.description;
    }
    if (row.active_dataset_version_id != null) {
      dto.active_dataset_version_id = row.active_dataset_version_id;
    }
    if (row.active_version_updated_at != null) {
      dto.active_version_updated_at = goTimestamptz(row.active_version_updated_at);
    }
    if (row.metadata != null && Object.keys(row.metadata).length > 0) {
      dto.metadata = row.metadata;
    }
    return dto;
  }
}
