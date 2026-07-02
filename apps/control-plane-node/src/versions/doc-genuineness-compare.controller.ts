import { Controller, Get, Param, Query } from '@nestjs/common';
import { httpError } from '../common/errors';
import { ArtifactViewsService } from './artifact-views.service';

/** 진성 분류 모델 비교 — dataset 스코프 경로 (Go handleCompareDocGenuineness). */
@Controller('projects/:project_id/datasets/:dataset_id/doc_genuineness')
export class DocGenuinenessCompareController {
  constructor(private readonly service: ArtifactViewsService) {}

  /** GET .../doc_genuineness/compare?version_id&model_a&model_b — 계약 동일. */
  @Get('compare')
  async compare(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Query('version_id') versionId?: string,
    @Query('model_a') modelA?: string,
    @Query('model_b') modelB?: string,
    @Query('limit') limit?: string,
    @Query('offset') offset?: string,
  ): Promise<unknown> {
    const vid = (versionId ?? '').trim();
    const a = (modelA ?? '').trim();
    const b = (modelB ?? '').trim();
    if (!vid || !a || !b) {
      throw httpError(400, 'version_id, model_a, model_b query params are required');
    }
    return this.service.compareDocGenuineness(
      projectId,
      datasetId,
      vid,
      a,
      b,
      ...parsePagination(limit, offset),
    );
  }
}

/** Go parseArtifactPagination — 잘못된 값은 default(100/0) fallback. */
function parsePagination(limitRaw?: string, offsetRaw?: string): [number, number] {
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
  return [limit, offset];
}
