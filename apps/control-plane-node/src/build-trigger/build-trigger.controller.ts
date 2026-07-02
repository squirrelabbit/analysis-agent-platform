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

  /** POST .../doc_genuineness — Go handleCreateDocGenuinenessJob 계약 동일. */
  @Post('doc_genuineness')
  @HttpCode(202)
  async docGenuineness(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
    @Body() body?: Record<string, unknown>,
  ): Promise<BuildJobAccepted> {
    return this.service.createDocGenuinenessJob(projectId, datasetId, versionId, body ?? {});
  }

  /** POST .../clause_label — Go handleCreateClauseLabelJob 계약 동일. */
  @Post('clause_label')
  @HttpCode(202)
  async clauseLabel(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
    @Body() body?: Record<string, unknown>,
  ): Promise<BuildJobAccepted> {
    return this.service.createClauseLabelJob(projectId, datasetId, versionId, body ?? {});
  }

  /** POST .../clause_keywords — Go handleCreateClauseKeywordsJob 계약 동일. */
  @Post('clause_keywords')
  @HttpCode(202)
  async clauseKeywords(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
    @Body() body?: Record<string, unknown>,
  ): Promise<BuildJobAccepted> {
    return this.service.createClauseKeywordsJob(projectId, datasetId, versionId, body ?? {});
  }

  /** POST .../{type}/cancel 3종 — Go handleCancelBuild 계약 동일 (202 cancelling). */
  @Post('doc_genuineness/cancel')
  @HttpCode(202)
  async cancelDocGenuineness(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
  ): Promise<unknown> {
    return this.service.cancelBuild(projectId, datasetId, versionId, 'doc_genuineness');
  }

  @Post('clause_label/cancel')
  @HttpCode(202)
  async cancelClauseLabel(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
  ): Promise<unknown> {
    return this.service.cancelBuild(projectId, datasetId, versionId, 'clause_label');
  }

  @Post('clause_keywords/cancel')
  @HttpCode(202)
  async cancelClauseKeywords(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
  ): Promise<unknown> {
    return this.service.cancelBuild(projectId, datasetId, versionId, 'clause_keywords');
  }
}
