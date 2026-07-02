import { HttpException, Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { httpError, notFound } from '../common/errors';
import { anyStringList } from '../common/metadata';
import { VersionsRepository } from '../versions/versions.repository';
import { BuildTriggerRepository } from './build-trigger.repository';
import { TemporalStarterService } from './temporal.service';

/** POST 응답 slim shape (Go DatasetBuildJobAccepted). */
export interface BuildJobAccepted {
  job_id: string;
  build_type: string;
  status: string;
  created_at: string;
}

/**
 * 쓰기 경로 1차 — dataset build 트리거 (Go CreateCleanJob 대응, clean만).
 * job row 생성 + version 상태 큐잉 + Temporal dataset.build.v1 start.
 * 워크플로/액티비티 실행은 기존 Go temporal-worker (PoC 3단계에서 interop 검증).
 * doc_genuineness/clause_label/clause_keywords 트리거는 다음 증분(모델/verify 검증 동반).
 */
@Injectable()
export class BuildTriggerService {
  constructor(
    private readonly versions: VersionsRepository,
    private readonly repo: BuildTriggerRepository,
    private readonly temporal: TemporalStarterService,
  ) {}

  async createCleanJob(
    projectId: string,
    datasetId: string,
    versionId: string,
    body: Record<string, unknown>,
  ): Promise<BuildJobAccepted> {
    const dataset = await this.versions.getDataset(projectId, datasetId);
    if (dataset === undefined) {
      throw notFound('dataset');
    }
    const row = await this.versions.get(projectId, versionId);
    if (row === undefined || row.dataset_id !== datasetId) {
      throw notFound('dataset version');
    }
    const meta = row.metadata ?? {};

    if (row.data_type === 'structured') {
      throw httpError(400, 'dataset clean requires unstructured or mixed dataset version');
    }
    // Go resolveDatasetBuildTextSelection — 입력 → raw_text_columns → text_columns.
    const inputColumns = anyStringList(body['text_columns']) ?? [];
    const columns =
      inputColumns.length > 0
        ? inputColumns
        : anyStringList(meta['raw_text_columns']) ?? anyStringList(meta['text_columns']) ?? [];
    if (columns.length === 0) {
      throw httpError(400, 'text_columns is required for dataset clean');
    }

    // idempotent — 같은 버전의 queued/running clean이 있으면 그 job을 그대로 반환.
    const active = await this.repo.findActiveJob(projectId, row.dataset_version_id, 'clean');
    if (active) {
      return {
        job_id: active.job_id,
        build_type: active.build_type,
        status: active.status,
        created_at: kstNow(new Date(pgToIso(active.created_at))),
      };
    }

    // request map — Go requestToMap(DatasetCleanRequest) omitempty 대응(보낸 필드만).
    const request: Record<string, unknown> = {};
    if (inputColumns.length > 0 || Array.isArray(body['text_columns'])) {
      const raw = anyStringList(body['text_columns']);
      if (raw) {
        request['text_columns'] = raw;
      }
    }
    if (typeof body['date_column'] === 'string' && body['date_column']) {
      request['date_column'] = body['date_column'];
    }

    const jobId = randomUUID();
    const createdAt = new Date();
    await this.repo.setVersionBuildQueued(projectId, row.dataset_version_id, 'clean');
    await this.repo.insertJob({
      job_id: jobId,
      project_id: projectId,
      dataset_id: datasetId,
      dataset_version_id: row.dataset_version_id,
      build_type: 'clean',
      status: 'queued',
      request,
      triggered_by: 'api',
      created_at: createdAt.toISOString(),
    });

    try {
      const workflowId = await this.temporal.startDatasetBuild({
        jobId,
        projectId,
        datasetId,
        datasetVersionId: row.dataset_version_id,
        buildType: 'clean',
        requestId: `dataset-build-request-${jobId}`,
      });
      await this.repo.setJobWorkflowId(jobId, workflowId);
    } catch (error) {
      const message = `failed to start dataset build workflow: ${String(
        (error as Error).message ?? error,
      )}`;
      await this.repo.markJobStartFailed(jobId, message);
      throw new HttpException({ detail: message }, 500);
    }

    return {
      job_id: jobId,
      build_type: 'clean',
      status: 'queued',
      created_at: kstNow(createdAt),
    };
  }
}

/** pg timestamptz text → ISO (Date 파싱용). */
function pgToIso(pgText: string): string {
  return pgText.replace(' ', 'T');
}

/** JS Date → KST RFC3339 (ms 정밀도, displaytime 규약과 동일 +09:00 표기). */
function kstNow(date: Date): string {
  const kst = new Date(date.getTime() + 9 * 3600_000);
  const pad = (n: number, w = 2) => String(n).padStart(w, '0');
  const ms = kst.getUTCMilliseconds();
  const frac = ms > 0 ? '.' + String(ms).padStart(3, '0').replace(/0+$/, '') : '';
  return (
    `${kst.getUTCFullYear()}-${pad(kst.getUTCMonth() + 1)}-${pad(kst.getUTCDate())}` +
    `T${pad(kst.getUTCHours())}:${pad(kst.getUTCMinutes())}:${pad(kst.getUTCSeconds())}${frac}+09:00`
  );
}
