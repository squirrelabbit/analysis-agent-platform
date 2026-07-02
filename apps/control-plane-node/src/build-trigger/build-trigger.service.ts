import { HttpException, Injectable } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { httpError, notFound } from '../common/errors';
import { lloaModelOptions } from '../common/lloa-models';
import { anyStringList, anyStringValue, isPlainObject, metadataString } from '../common/metadata';
import { cleanStatus } from '../versions/versions.service';
import { VersionsRepository } from '../versions/versions.repository';
import { PythonWorkerClient } from '../worker/worker-client';
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
 * 쓰기 경로 — dataset build 트리거 4종 + cancel (Go Create*Job/CancelDatasetBuild 대응).
 * job row 생성 + version 상태 큐잉 + Temporal dataset.build.v1 start.
 * 워크플로/액티비티 실행은 기존 Go temporal-worker (PoC 3단계에서 interop 검증).
 */
@Injectable()
export class BuildTriggerService {
  constructor(
    private readonly versions: VersionsRepository,
    private readonly repo: BuildTriggerRepository,
    private readonly temporal: TemporalStarterService,
    private readonly worker: PythonWorkerClient,
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

    // request map — Go requestToMap(DatasetCleanRequest) omitempty 대응(보낸 필드만).
    const request: Record<string, unknown> = {};
    const rawColumns = anyStringList(body['text_columns']);
    if (rawColumns) {
      request['text_columns'] = rawColumns;
    }
    if (typeof body['date_column'] === 'string' && body['date_column']) {
      request['date_column'] = body['date_column'];
    }
    return this.enqueue(projectId, datasetId, row.dataset_version_id, 'clean', request);
  }

  /** Go CreateDocGenuinenessJob — subject 설정 + 모델/verify 검증 + clean ready. */
  async createDocGenuinenessJob(
    projectId: string,
    datasetId: string,
    versionId: string,
    body: Record<string, unknown>,
  ): Promise<BuildJobAccepted> {
    const { row, meta, dataset } = await this.loadVersion(projectId, datasetId, versionId);
    validateDocGenuinenessConfig(dataset.metadata ?? {});
    validateModelSelection(body);
    requireCleanReady(row, meta, 'doc_genuineness');
    const request = pickRequestFields(body, [
      'doc_genuineness_prompt_version',
      'model_id',
      'verify',
      'classify_models',
      'judge_model',
      'force',
    ]);
    return this.enqueue(projectId, datasetId, row.dataset_version_id, 'doc_genuineness', request);
  }

  /** Go CreateClauseLabelJob — 모델/verify 검증 + clean ready. */
  async createClauseLabelJob(
    projectId: string,
    datasetId: string,
    versionId: string,
    body: Record<string, unknown>,
  ): Promise<BuildJobAccepted> {
    const { row, meta } = await this.loadVersion(projectId, datasetId, versionId);
    validateModelSelection(body);
    requireCleanReady(row, meta, 'clause_label');
    const request = pickRequestFields(body, [
      'clause_label_prompt_version',
      'include_genuineness',
      'model_id',
      'force',
      'verify',
      'classify_models',
      'judge_model',
    ]);
    return this.enqueue(projectId, datasetId, row.dataset_version_id, 'clause_label', request);
  }

  /** Go CreateClauseKeywordsJob — precondition은 clause_label artifact 존재. */
  async createClauseKeywordsJob(
    projectId: string,
    datasetId: string,
    versionId: string,
    body: Record<string, unknown>,
  ): Promise<BuildJobAccepted> {
    const { row, meta } = await this.loadVersion(projectId, datasetId, versionId);
    let clauseRef = metadataString(meta, 'clause_label_ref');
    if (!clauseRef) {
      clauseRef = metadataString(meta, 'clause_label_uri');
    }
    if (!clauseRef) {
      throw httpError(400, 'clause_label must be ready before clause_keywords');
    }
    const request = pickRequestFields(body, ['keyword_min_len']);
    return this.enqueue(projectId, datasetId, row.dataset_version_id, 'clause_keywords', request);
  }

  /** Go CancelDatasetBuild — worker에 협조적 취소 신호(부분 결과 보존). */
  async cancelBuild(
    projectId: string,
    datasetId: string,
    versionId: string,
    buildType: string,
  ): Promise<{ status: string; build_type: string }> {
    const { row } = await this.loadVersion(projectId, datasetId, versionId);
    const active = await this.repo.findActiveJob(projectId, row.dataset_version_id, buildType);
    if (!active) {
      throw httpError(400, 'no running build to cancel for this version/type');
    }
    const result = await this.worker.postTask('cancel', {
      dataset_version_id: row.dataset_version_id,
    });
    if (result.status >= 400) {
      throw new HttpException({ detail: `build cancel failed: HTTP ${result.status}` }, 500);
    }
    return { status: 'cancelling', build_type: buildType };
  }

  private async loadVersion(projectId: string, datasetId: string, versionId: string) {
    const dataset = await this.versions.getDataset(projectId, datasetId);
    if (dataset === undefined) {
      throw notFound('dataset');
    }
    const row = await this.versions.get(projectId, versionId);
    if (row === undefined || row.dataset_id !== datasetId) {
      throw notFound('dataset version');
    }
    return { row, meta: row.metadata ?? {}, dataset };
  }

  /** 공통 접수 코어 — idempotent 반환 → 상태 큐잉 → job insert → Temporal start. */
  private async enqueue(
    projectId: string,
    datasetId: string,
    datasetVersionId: string,
    buildType: string,
    request: Record<string, unknown>,
  ): Promise<BuildJobAccepted> {
    const active = await this.repo.findActiveJob(projectId, datasetVersionId, buildType);
    if (active) {
      return {
        job_id: active.job_id,
        build_type: active.build_type,
        status: active.status,
        created_at: kstNow(new Date(pgToIso(active.created_at))),
      };
    }
    const jobId = randomUUID();
    const createdAt = new Date();
    await this.repo.setVersionBuildQueued(projectId, datasetVersionId, buildType);
    await this.repo.insertJob({
      job_id: jobId,
      project_id: projectId,
      dataset_id: datasetId,
      dataset_version_id: datasetVersionId,
      build_type: buildType,
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
        datasetVersionId,
        buildType,
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
    return { job_id: jobId, build_type: buildType, status: 'queued', created_at: kstNow(createdAt) };
  }
}

/** Go extractDocGenuinenessConfig의 검증부 — subject_name 필수 (no festival fallback). */
function validateDocGenuinenessConfig(datasetMeta: Record<string, unknown>): void {
  const raw = datasetMeta['doc_genuineness'];
  const subjectName = isPlainObject(raw) ? anyStringValue(raw['subject_name']).trim() : '';
  if (!subjectName) {
    throw httpError(
      400,
      'dataset.metadata.doc_genuineness.subject_name is required — set metadata.doc_genuineness in POST /projects/{pid}/datasets or PATCH /projects/{pid}/datasets/{did}/metadata before doc_genuineness build (no festival fallback)',
    );
  }
}

/** Go validateVerifyModels / validateLLOAModelID — verify면 2모델+judge, 아니면 단일 model_id. */
function validateModelSelection(body: Record<string, unknown>): void {
  const options = lloaModelOptions();
  const allowed = new Set(options.map((option) => option.model_id));
  if (body['verify'] === true) {
    if (options.length === 0) {
      throw httpError(400, 'verify mode requires LLOA model allowlist (config/lloa_models.json)');
    }
    const classify = (anyStringList(body['classify_models']) ?? []).slice();
    if (classify.length !== 2 || classify[0] === classify[1]) {
      throw httpError(400, 'verify mode requires classify_models = 2 distinct allowlisted model ids');
    }
    for (const model of classify) {
      if (!allowed.has(model)) {
        throw httpError(400, `model_id not allowed: ${model}`);
      }
    }
    const judge = typeof body['judge_model'] === 'string' ? body['judge_model'].trim() : '';
    if (judge && !allowed.has(judge)) {
      throw httpError(400, `model_id not allowed: ${judge}`);
    }
    return;
  }
  const modelId = typeof body['model_id'] === 'string' ? body['model_id'].trim() : '';
  if (!modelId) {
    return; // 생략 시 worker env default — 통과 (Go validateLLOAModelID).
  }
  if (!allowed.has(modelId)) {
    const list = options.map((option) => option.model_id).join(', ');
    throw httpError(400, `model_id not allowed: ${modelId} (allowed: ${list})`);
  }
}

/** Go Create*Job의 clean ready precondition. */
function requireCleanReady(
  row: { data_type: string; clean_status: string | null },
  meta: Record<string, unknown>,
  buildType: string,
): void {
  const status = cleanStatus(row as never, meta);
  if (status === 'queued' || status === 'cleaning' || status === 'failed' || status === 'stale') {
    throw httpError(400, `dataset clean must be ready before ${buildType}`);
  }
}

/** Go requestToMap(struct omitempty) 대응 — 알려진 키만, 값이 있는 것만 담는다. */
function pickRequestFields(
  body: Record<string, unknown>,
  keys: string[],
): Record<string, unknown> {
  const request: Record<string, unknown> = {};
  for (const key of keys) {
    const value = body[key];
    if (value === undefined || value === null) {
      continue;
    }
    if (Array.isArray(value)) {
      if (value.length > 0) {
        request[key] = value; // Go []string omitempty — 빈 배열은 생략
      }
      continue;
    }
    if (typeof value === 'string' && value === '') {
      request[key] = value; // *string은 non-nil이면 빈 값도 유지 (Go 포인터 시맨틱)
      continue;
    }
    request[key] = value;
  }
  return request;
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
