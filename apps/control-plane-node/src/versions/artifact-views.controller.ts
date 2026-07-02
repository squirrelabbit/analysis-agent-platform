import { Controller, Get, Param, Query } from '@nestjs/common';
import { DatasetArtifactViewDto } from './artifact-view.dto';
import { ArtifactViewsService } from './artifact-views.service';

@Controller('projects/:project_id/datasets/:dataset_id/versions/:version_id')
export class ArtifactViewsController {
  constructor(private readonly service: ArtifactViewsService) {}

  /** GET .../clean — Go handleGetCleanView 계약 동일. */
  @Get('clean')
  async clean(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
  ): Promise<DatasetArtifactViewDto> {
    return this.service.cleanView(projectId, datasetId, versionId);
  }

  /** GET .../doc_genuineness — Go handleGetDocGenuinenessView 계약 동일. */
  @Get('doc_genuineness')
  async docGenuineness(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
    @Query('limit') limit?: string,
    @Query('offset') offset?: string,
    @Query('genuineness') genuineness?: string,
    @Query('disagreement') disagreement?: string,
    @Query('needs_review') needsReview?: string,
  ): Promise<DatasetArtifactViewDto> {
    return this.service.docGenuinenessView(projectId, datasetId, versionId, {
      ...parseArtifactPagination(limit, offset),
      genuineness: (genuineness ?? '').trim(),
      disagreementOnly: disagreement === 'true',
      needsReviewOnly: needsReview === 'true',
    });
  }

  /** GET .../doc_genuineness/runs — Go handleListDocGenuinenessRuns 계약 동일. */
  @Get('doc_genuineness/runs')
  async docGenuinenessRuns(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
  ): Promise<unknown> {
    return this.service.docGenuinenessRuns(projectId, datasetId, versionId);
  }

  /** GET .../clause_keywords — Go handleGetClauseKeywordsView 계약 동일. */
  @Get('clause_keywords')
  async clauseKeywords(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
    @Query('limit') limit?: string,
    @Query('offset') offset?: string,
    @Query('aspect') aspect?: string,
    @Query('sentiment') sentiment?: string,
    @Query('q') q?: string,
    @Query('group') group?: string,
  ): Promise<DatasetArtifactViewDto> {
    return this.service.clauseKeywordsView(projectId, datasetId, versionId, {
      ...parseArtifactPagination(limit, offset),
      aspect: (aspect ?? '').trim(),
      sentiment: (sentiment ?? '').trim(),
      q: (q ?? '').trim(),
      group: (group ?? '').trim(),
    });
  }

  /** GET .../clause_label — Go handleGetClauseLabelView 계약 동일. */
  @Get('clause_label')
  async clauseLabel(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Param('version_id') versionId: string,
    @Query('limit') limit?: string,
    @Query('offset') offset?: string,
    @Query('aspect') aspect?: string,
    @Query('sentiment') sentiment?: string,
    @Query('disagreement') disagreement?: string,
    @Query('needs_review') needsReview?: string,
  ): Promise<DatasetArtifactViewDto> {
    return this.service.clauseLabelView(projectId, datasetId, versionId, {
      ...parseArtifactPagination(limit, offset),
      aspect: (aspect ?? '').trim(),
      sentiment: (sentiment ?? '').trim(),
      disagreementOnly: disagreement === 'true',
      needsReviewOnly: needsReview === 'true',
    });
  }
}

/** Go parseArtifactPagination — 잘못된 값은 default(100/0) fallback. */
function parseArtifactPagination(
  limitRaw?: string,
  offsetRaw?: string,
): { limit: number; offset: number } {
  let limit = 100;
  let offset = 0;
  if (limitRaw) {
    const parsed = Number.parseInt(limitRaw, 10);
    if (Number.isInteger(parsed) && parsed > 0) {
      limit = parsed;
    }
  }
  if (offsetRaw) {
    const parsed = Number.parseInt(offsetRaw, 10);
    if (Number.isInteger(parsed) && parsed >= 0) {
      offset = parsed;
    }
  }
  return { limit, offset };
}
