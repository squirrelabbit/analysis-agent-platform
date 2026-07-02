import { Body, Controller, HttpCode, Param, Post } from '@nestjs/common';
import { BuildJobAccepted, BuildTriggerService } from './build-trigger.service';

@Controller('projects/:project_id/datasets/:dataset_id/versions/:version_id')
export class BuildTriggerController {
  constructor(private readonly service: BuildTriggerService) {}

  /** POST .../clean — Go handleCreateCleanJob 계약 동일 (202 + slim accepted). */
  @Post('clean')
  @HttpCode(202)
  async clean(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
    @Body() body?: Record<string, unknown>,
  ): Promise<BuildJobAccepted> {
    return this.service.createCleanJob(projectId, datasetId, versionId, body ?? {});
  }
}
