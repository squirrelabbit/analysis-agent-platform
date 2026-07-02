import { Controller, Get, Param } from '@nestjs/common';
import { DatasetVersionDetailDto, DatasetVersionListResponse } from './version.dto';
import { VersionsService } from './versions.service';

@Controller('projects/:project_id/datasets/:dataset_id/versions')
export class VersionsController {
  constructor(private readonly service: VersionsService) {}

  /** GET /projects/{pid}/datasets/{did}/versions — Go handleListDatasetVersions 계약 동일. */
  @Get()
  async list(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
  ): Promise<DatasetVersionListResponse> {
    return this.service.list(projectId, datasetId);
  }

  /** GET /versions/{version_id} — Go handleGetDatasetVersion(→ GetDatasetVersionDetail) 계약 동일. */
  @Get(':version_id')
  async detail(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
  ): Promise<DatasetVersionDetailDto> {
    return this.service.detail(projectId, datasetId, versionId);
  }
}
