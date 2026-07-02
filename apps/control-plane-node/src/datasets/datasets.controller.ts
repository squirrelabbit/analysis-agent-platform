import { Controller, Get, Param } from '@nestjs/common';
import { DatasetDto, DatasetListResponse } from './dataset.dto';
import { DatasetsService } from './datasets.service';

@Controller('projects/:project_id/datasets')
export class DatasetsController {
  constructor(private readonly service: DatasetsService) {}

  /** GET /projects/{pid}/datasets — Go handleListDatasets 계약 동일. */
  @Get()
  async list(@Param('project_id') projectId: string): Promise<DatasetListResponse> {
    return this.service.list(projectId);
  }

  /** GET /projects/{pid}/datasets/{did} — Go handleGetDataset 계약 동일. */
  @Get(':dataset_id')
  async get(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
  ): Promise<DatasetDto> {
    return this.service.get(projectId, datasetId);
  }
}
