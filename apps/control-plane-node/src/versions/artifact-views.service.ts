import { HttpException, Injectable } from '@nestjs/common';
import { existsSync } from 'node:fs';
import { httpError, notFound } from '../common/errors';
import { goRfc3339, goTimestamptz, pgEpochMicros } from '../common/go-time';
import { modelDisplayNameFor } from '../common/lloa-models';
import {
  anyStringList,
  anyStringValue,
  intMapValue,
  intValueOrZero,
  isPlainObject,
  metadataBool,
  metadataString,
} from '../common/metadata';
import { loadBuildProgress } from '../common/progress';
import { rewriteWorkspacePath } from '../common/workspace-path';
import { PythonWorkerClient } from '../worker/worker-client';
import {
  ArtifactProgressDto,
  DatasetArtifactViewDto,
} from './artifact-view.dto';
import {
  ArtifactViewsRepository,
  ClauseLabelOverrideRow,
  DocGenuinenessOverrideRow,
  LatestBuildJobRow,
} from './artifact-views.repository';
import { KeywordDictionaryRepository } from '../keyword-dictionary/keyword-dictionary.repository';
import { cleanStatus } from './versions.service';
import { DatasetActiveRow, DatasetVersionRow, VersionsRepository } from './versions.repository';

export interface ArtifactViewQuery {
  limit: number;
  offset: number;
  genuineness?: string;
  aspect?: string;
  sentiment?: string;
  disagreementOnly?: boolean;
  needsReviewOnly?: boolean;
}

/** worker artifact view task 응답 (Go 로더 반환 대응). */
interface WorkerViewResult {
  summary: Record<string, unknown>;
  prompt_version: string;
  total: number;
  items: Record<string, unknown>[];
}

@Injectable()
export class ArtifactViewsService {
  constructor(
    private readonly versions: VersionsRepository,
    private readonly repo: ArtifactViewsRepository,
    private readonly keywordDictionary: KeywordDictionaryRepository,
    private readonly worker: PythonWorkerClient,
  ) {}

  /** Go GetCleanView — items/pagination 없이 status + summary(완료 시)만. */
  async cleanView(
    projectId: string,
    datasetId: string,
    versionId: string,
  ): Promise<DatasetArtifactViewDto> {
    const { row, meta } = await this.loadVersion(projectId, datasetId, versionId);
    const job = await this.repo.latestJob(projectId, row.dataset_version_id, 'clean');

    let ref = metadataString(meta, 'cleaned_ref');
    if (!ref) {
      ref = metadataString(meta, 'clean_uri');
    }
    const view = this.baseView('clean', job, meta);
    view.status = resolveArtifactStatus(ref, job, cleanStatus(row, meta), false);

    const summary = cleanViewSummary(meta);
    if (summary && view.status === 'completed') {
      view.summary = summary;
    }
    return view;
  }

  /** Go GetDocGenuinenessView. */
  async docGenuinenessView(
    projectId: string,
    datasetId: string,
    versionId: string,
    query: ArtifactViewQuery,
  ): Promise<DatasetArtifactViewDto> {
    const { row, meta } = await this.loadVersion(projectId, datasetId, versionId);
    const { limit, offset } = normalizeArtifactPagination(query.limit, query.offset);
    const job = await this.repo.latestJob(projectId, row.dataset_version_id, 'doc_genuineness');

    let ref = metadataString(meta, 'doc_genuineness_ref');
    if (!ref) {
      ref = metadataString(meta, 'doc_genuineness_uri');
    }
    const view = this.baseView('doc_genuineness', job, meta);
    view.status = resolveArtifactStatus(
      ref,
      job,
      metadataString(meta, 'doc_genuineness_status'),
      metadataBool(meta, 'doc_genuineness_cancelled'),
    );
    view.pagination = { limit, offset, total: 0 };

    if (!ref || !existsSync(rewriteWorkspacePath(ref))) {
      return view;
    }
    const cleanRef = resolveCleanRef(meta);
    const verifyMode = metadataString(meta, 'doc_genuineness_mode') === 'verify';

    const result = await this.callWorker('artifact_doc_genuineness_view', {
      ref,
      clean_ref: cleanRef,
      limit,
      offset,
      mode: verifyMode ? 'verify' : 'single',
      genuineness: query.genuineness ?? '',
      disagreement_only: query.disagreementOnly === true,
      needs_review_only: query.needsReviewOnly === true,
    });

    view.summary = result.summary;
    const applied: Record<string, unknown> = {};
    if (result.prompt_version) {
      applied['prompt_version'] = result.prompt_version;
    }
    if (verifyMode) {
      view.summary['mode'] = 'verify';
      const storedApplied = summaryMetadataMap(meta, 'doc_genuineness_summary', 'applied');
      if (storedApplied) {
        Object.assign(applied, storedApplied);
      }
      for (const key of [
        'agreement_count',
        'disagreement_count',
        'judge_count',
        'revised_count',
        'review_count',
        'classify_error_count',
        'models',
      ]) {
        const value = summaryMetadataValue(meta, 'doc_genuineness_summary', key);
        if (value.present) {
          view.summary[key] = value.value;
        }
      }
    } else {
      const model = summaryMetadataString(meta, 'doc_genuineness_summary', 'model');
      if (model) {
        applied['model'] = model;
      }
      const display = modelDisplayNameFor(model);
      if (display) {
        applied['model_display_name'] = display;
      }
    }
    if (Object.keys(applied).length > 0) {
      view.applied = applied;
    }
    if (result.items.length > 0) {
      view.items = result.items;
    }
    view.pagination.total = result.total;

    const overrides = await this.repo.listDocGenuinenessOverrides(projectId, row.dataset_version_id);
    const crossed = applyDocGenuinenessOverrides(view, overrides);
    if (view.summary) {
      const clauseReady =
        metadataString(meta, 'clause_label_status') === 'ready' ||
        metadataString(meta, 'clause_keywords_status') === 'ready';
      view.summary['downstream_rerun_recommended'] = crossed && clauseReady;
    }
    return view;
  }

  /** Go GetClauseLabelView. */
  async clauseLabelView(
    projectId: string,
    datasetId: string,
    versionId: string,
    query: ArtifactViewQuery,
  ): Promise<DatasetArtifactViewDto> {
    const { row, meta } = await this.loadVersion(projectId, datasetId, versionId);
    const { limit, offset } = normalizeArtifactPagination(query.limit, query.offset);
    const job = await this.repo.latestJob(projectId, row.dataset_version_id, 'clause_label');

    let ref = metadataString(meta, 'clause_label_ref');
    if (!ref) {
      ref = metadataString(meta, 'clause_label_uri');
    }
    const view = this.baseView('clause_label', job, meta);
    view.status = resolveArtifactStatus(
      ref,
      job,
      metadataString(meta, 'clause_label_status'),
      metadataBool(meta, 'clause_label_cancelled'),
    );
    view.pagination = { limit, offset, total: 0 };

    // 진행 중(running/queued) 빌드의 "이번 실행" 메타 — artifact가 아직 없어도 노출.
    const runningApplied = runningClauseLabelApplied(job);
    if (runningApplied) {
      view.applied = runningApplied;
    }

    if (!ref || !existsSync(rewriteWorkspacePath(ref))) {
      return view;
    }
    let prompt = metadataString(meta, 'clause_label_prompt_version');
    const verifyMode = metadataString(meta, 'clause_label_mode') === 'verify';
    const cleanRef = resolveCleanRef(meta);

    const result = await this.callWorker('artifact_clause_label_view', {
      ref,
      clean_ref: cleanRef,
      limit,
      offset,
      mode: verifyMode ? 'verify' : 'single',
      aspect: query.aspect ?? '',
      sentiment: query.sentiment ?? '',
      disagreement_only: query.disagreementOnly === true,
      needs_review_only: query.needsReviewOnly === true,
    });

    view.summary = result.summary;
    if (!prompt) {
      prompt = result.prompt_version;
    }
    const applied: Record<string, unknown> = {};
    if (verifyMode) {
      view.summary['mode'] = 'verify';
      if (!prompt) {
        prompt = summaryMetadataString(meta, 'clause_label_summary', 'prompt_version');
      }
      const storedApplied = summaryMetadataMap(meta, 'clause_label_summary', 'applied');
      if (storedApplied) {
        Object.assign(applied, storedApplied);
      }
      for (const key of [
        'resolution_counts',
        'models',
        'chunking',
        'dropped_irrelevant_count',
        'judge_prompt_version',
      ]) {
        const value = summaryMetadataValue(meta, 'clause_label_summary', key);
        if (value.present) {
          view.summary[key] = value.value;
        }
      }
    } else {
      const model = summaryMetadataString(meta, 'clause_label_summary', 'model');
      if (model) {
        applied['model'] = model;
      }
      const display = modelDisplayNameFor(model);
      if (display) {
        applied['model_display_name'] = display;
      }
    }
    if (prompt) {
      applied['prompt_version'] = prompt;
    }
    const taxonomyId = summaryMetadataString(meta, 'clause_label_summary', 'taxonomy_id');
    if (taxonomyId) {
      applied['taxonomy_id'] = taxonomyId;
    }
    if (runningApplied) {
      Object.assign(applied, runningApplied);
    }
    if (Object.keys(applied).length > 0) {
      view.applied = applied;
    } else {
      delete view.applied;
    }
    if (result.items.length > 0) {
      view.items = result.items;
    }
    view.pagination.total = result.total;

    const overrides = await this.repo.listClauseLabelOverrides(projectId, row.dataset_version_id);
    applyClauseLabelOverrides(view, overrides);
    return view;
  }

  /** Go GetClauseKeywordsView — 사전 overlay + 대시보드 집계는 worker, 합성은 여기. */
  async clauseKeywordsView(
    projectId: string,
    datasetId: string,
    versionId: string,
    query: ArtifactViewQuery & { q?: string; group?: string },
  ): Promise<DatasetArtifactViewDto> {
    const { row, meta, dataset } = await this.loadVersion(projectId, datasetId, versionId);
    const { limit, offset } = normalizeArtifactPagination(query.limit, query.offset);
    const job = await this.repo.latestJob(projectId, row.dataset_version_id, 'clause_keywords');

    let ref = metadataString(meta, 'clause_keywords_ref');
    if (!ref) {
      ref = metadataString(meta, 'clause_keywords_uri');
    }
    const view = this.baseView('clause_keywords', job, meta);
    view.status = resolveArtifactStatus(
      ref,
      job,
      metadataString(meta, 'clause_keywords_status'),
      metadataBool(meta, 'clause_keywords_cancelled'),
    );
    view.pagination = { limit, offset, total: 0 };

    if (!ref || !existsSync(rewriteWorkspacePath(ref))) {
      return view;
    }
    // 키워드 정제 사전 — 활성 규칙을 worker overlay로 전달(원본 artifact 불변).
    const rules = await this.keywordDictionary.listRules(projectId, datasetId, true);
    const result = await this.callWorker('artifact_clause_keywords_view', {
      ref,
      limit,
      offset,
      aspect: query.aspect ?? '',
      sentiment: query.sentiment ?? '',
      q: query.q ?? '',
      group: query.group ?? '',
      rules: rules.map((rule) => ({
        rule_type: rule.rule_type,
        source_term: rule.source_term,
        target_term: rule.target_term,
        active: rule.active,
      })),
    });

    view.summary = result.summary;
    const activeRuleCount = rules.filter((rule) => rule.active).length;
    if (activeRuleCount > 0) {
      view.summary['dictionary_rule_count'] = activeRuleCount;
    }
    // 추천 제외어 — 검색어/대상명(dataset.metadata.doc_genuineness) 유래 키워드 표시.
    const terms = subjectDerivedTerms(dataset.metadata ?? {});
    if (terms.size > 0) {
      const flagged = annotateSuggestedExclude(result.items, terms);
      if (flagged > 0) {
        view.summary['suggested_exclude_page_count'] = flagged;
      }
      view.summary['suggested_exclude_terms'] = [...terms].sort();
    }
    const extractorVersion = summaryMetadataString(meta, 'clause_keywords_summary', 'extractor_version');
    if (extractorVersion) {
      view.applied = { extractor_version: extractorVersion };
    }
    if (result.items.length > 0) {
      view.items = result.items;
    }
    view.pagination.total = result.total;
    return view;
  }

  /** Go GetDocGenuinenessRuns — 모델별 결과 목록 (비교 화면 dropdown용). */
  async docGenuinenessRuns(
    projectId: string,
    datasetId: string,
    versionId: string,
  ): Promise<Record<string, unknown>> {
    const { meta } = await this.loadVersion(projectId, datasetId, versionId);
    const items = docGenuinenessRunsFromMetadata(meta).map((run) => {
      const dto: Record<string, unknown> = { model: run.model };
      const display = modelDisplayNameFor(run.model);
      if (display) {
        dto['model_display_name'] = display;
      }
      dto['ref'] = run.ref;
      if (run.promptVersion) {
        dto['prompt_version'] = run.promptVersion;
      }
      dto['completed_at'] = run.completedAt;
      return dto;
    });
    return { dataset_version_id: versionId, items };
  }

  /** Go CompareDocGenuineness — 한 버전 안의 두 모델 결과 1:1 비교. */
  async compareDocGenuineness(
    projectId: string,
    datasetId: string,
    versionId: string,
    modelA: string,
    modelB: string,
    rawLimit: number,
    rawOffset: number,
  ): Promise<Record<string, unknown>> {
    if (modelA === modelB) {
      throw httpError(400, 'model_a and model_b must differ');
    }
    const { limit, offset } = normalizeArtifactPagination(rawLimit, rawOffset);
    const { row, meta } = await this.loadVersion(projectId, datasetId, versionId);
    const runs = docGenuinenessRunsFromMetadata(meta);
    const runA = runs.find((run) => run.model === modelA);
    if (!runA) {
      throw httpError(400, `no doc_genuineness result for model_a: ${modelA}`);
    }
    const runB = runs.find((run) => run.model === modelB);
    if (!runB) {
      throw httpError(400, `no doc_genuineness result for model_b: ${modelB}`);
    }
    const cleanRef = resolveCleanRef(meta);
    const rowsA = await this.loadRunLabels(runA.ref, cleanRef);
    const rowsB = await this.loadRunLabels(runB.ref, cleanRef);
    const overrides = await this.repo.listDocGenuinenessOverrides(projectId, row.dataset_version_id);
    const overrideByDoc = new Map(overrides.map((o) => [o.doc_id, o.override_genuineness]));

    const tiers = ['genuine_review', 'non_review', 'uncertain'];
    const tierIndex = new Map(tiers.map((tier, index) => [tier, index]));
    const confusion = tiers.map(() => tiers.map(() => 0));

    const docIds = [...new Set([...rowsA.keys(), ...rowsB.keys()])].sort();
    const disagreements: Record<string, unknown>[] = [];
    let compared = 0;
    let matched = 0;
    let onlyInA = 0;
    let onlyInB = 0;
    let ovSample = 0;
    let ovACorrect = 0;
    let ovBCorrect = 0;
    let unreviewed = 0;
    for (const docId of docIds) {
      const a = rowsA.get(docId);
      const b = rowsB.get(docId);
      if (a && !b) {
        onlyInA++;
        continue;
      }
      if (!a && b) {
        onlyInB++;
        continue;
      }
      if (!a || !b) {
        continue;
      }
      compared++;
      const ai = tierIndex.get(a.label);
      const bi = tierIndex.get(b.label);
      if (ai !== undefined && bi !== undefined) {
        confusion[ai][bi]++;
      }
      const truth = overrideByDoc.get(docId) ?? '';
      if (truth !== '') {
        ovSample++;
        if (a.label === truth) {
          ovACorrect++;
        }
        if (b.label === truth) {
          ovBCorrect++;
        }
      }
      if (a.label === b.label) {
        matched++;
        continue;
      }
      if (truth === '') {
        unreviewed++;
      }
      const item: Record<string, unknown> = { doc_id: docId, a_genuineness: a.label };
      if (a.reason) {
        item['a_reason'] = a.reason;
      }
      item['b_genuineness'] = b.label;
      if (b.reason) {
        item['b_reason'] = b.reason;
      }
      const cleaned = a.cleanedText || b.cleanedText;
      if (cleaned) {
        item['cleaned_text'] = cleaned;
      }
      if (truth) {
        item['override_genuineness'] = truth;
      }
      disagreements.push(item);
    }

    const rate = compared > 0 ? matched / compared : 0;
    const patterns: Record<string, unknown>[] = [];
    for (let i = 0; i < tiers.length; i++) {
      for (let j = 0; j < tiers.length; j++) {
        if (i !== j && confusion[i][j] > 0) {
          patterns.push({ a_genuineness: tiers[i], b_genuineness: tiers[j], count: confusion[i][j] });
        }
      }
    }
    patterns.sort((x, y) => (y['count'] as number) - (x['count'] as number));

    let overrideEval: Record<string, unknown> | null = null;
    if (ovSample > 0) {
      overrideEval = {
        sample_count: ovSample,
        a_correct: ovACorrect,
        b_correct: ovBCorrect,
        a_accuracy: ovACorrect / ovSample,
        b_accuracy: ovBCorrect / ovSample,
        leader: ovACorrect > ovBCorrect ? 'a' : ovBCorrect > ovACorrect ? 'b' : 'tie',
      };
    }
    const verdictLevel =
      overrideEval !== null ? 'ground_truth' : rate >= 0.85 ? 'agreement_only' : 'review_needed';

    const total = disagreements.length;
    const start = Math.min(offset, total);
    const end = Math.min(start + limit, total);

    return {
      version_a: {
        dataset_version_id: versionId,
        ...(modelA ? { model: modelA } : {}),
        ...(modelDisplayNameFor(modelA) ? { model_display_name: modelDisplayNameFor(modelA) } : {}),
        total: rowsA.size,
      },
      version_b: {
        dataset_version_id: versionId,
        ...(modelB ? { model: modelB } : {}),
        ...(modelDisplayNameFor(modelB) ? { model_display_name: modelDisplayNameFor(modelB) } : {}),
        total: rowsB.size,
      },
      tiers,
      compared,
      matched,
      agreement_rate: rate,
      only_in_a: onlyInA,
      only_in_b: onlyInB,
      confusion,
      disagreements: disagreements.slice(start, end),
      disagreements_total: total,
      pagination: { limit, offset, total },
      patterns,
      ...(overrideEval !== null ? { override_eval: overrideEval } : {}),
      unreviewed_disagreements: unreviewed,
      verdict_level: verdictLevel,
    };
  }

  /** Go loadRunLabels — run artifact 전체 doc 라벨을 map으로 (worker 단일모드 전체 로드). */
  private async loadRunLabels(
    ref: string,
    cleanRef: string,
  ): Promise<Map<string, { label: string; reason: string; cleanedText: string }>> {
    // Go는 limit 1<<30으로 전체를 읽는다 — pagination normalize를 우회한다.
    const result = await this.callWorker('artifact_doc_genuineness_view', {
      ref,
      clean_ref: cleanRef,
      limit: 1 << 30,
      offset: 0,
      mode: 'single',
      genuineness: '',
      disagreement_only: false,
      needs_review_only: false,
    });
    const rows = new Map<string, { label: string; reason: string; cleanedText: string }>();
    for (const item of result.items) {
      const docId = typeof item['doc_id'] === 'string' ? item['doc_id'] : '';
      if (!docId) {
        continue;
      }
      rows.set(docId, {
        label: typeof item['genuineness'] === 'string' ? item['genuineness'] : '',
        reason: typeof item['reason'] === 'string' ? item['reason'] : '',
        cleanedText: typeof item['cleaned_text'] === 'string' ? item['cleaned_text'] : '',
      });
    }
    return rows;
  }

  /** Go GetDatasetVersion의 404 정합성 (dataset → dataset version 순). */
  private async loadVersion(
    projectId: string,
    datasetId: string,
    versionId: string,
  ): Promise<{ row: DatasetVersionRow; meta: Record<string, unknown>; dataset: DatasetActiveRow }> {
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

  /** view 공통 필드 (Go DatasetArtifactView 기본값 + enrichViewWithJob). */
  private baseView(
    buildType: string,
    job: LatestBuildJobRow | undefined,
    meta: Record<string, unknown>,
  ): DatasetArtifactViewDto {
    const view: DatasetArtifactViewDto = {
      build_type: buildType,
      status: '',
      job_id: null,
      started_at: null,
      completed_at: null,
      duration_seconds: null,
      error_message: null,
    };
    if (job) {
      view.job_id = job.job_id;
      view.started_at = job.started_at != null ? goTimestamptz(job.started_at) : null;
      view.completed_at = job.completed_at != null ? goTimestamptz(job.completed_at) : null;
      view.error_message = job.error_message;
      view.duration_seconds = computeDurationSeconds(job.started_at, job.completed_at);
    }
    const raw = loadBuildProgress(meta, buildType);
    if (raw) {
      const progress: ArtifactProgressDto = { percent: raw.percent };
      if (raw.processed_rows !== 0) {
        progress.processed_rows = raw.processed_rows;
      }
      if (raw.total_rows !== 0) {
        progress.total_rows = raw.total_rows;
      }
      if (raw.eta_seconds !== null) {
        progress.eta_seconds = raw.eta_seconds;
      }
      if (raw.message) {
        progress.message = raw.message;
      }
      if (raw.updated_at !== null) {
        progress.updated_at = raw.updated_at;
      }
      view.progress = progress;
    }
    return view;
  }

  private async callWorker(
    task:
      | 'artifact_doc_genuineness_view'
      | 'artifact_clause_label_view'
      | 'artifact_clause_keywords_view',
    payload: Record<string, unknown>,
  ): Promise<WorkerViewResult> {
    let body: Record<string, unknown>;
    try {
      body = await this.worker.artifactView(task, payload);
    } catch (error) {
      // Go 로더(in-process DuckDB) 실패와 동일한 의미 — 조회 실패로 fail-loud.
      throw new HttpException({ detail: String((error as Error).message ?? error) }, 500);
    }
    return {
      summary: isPlainObject(body.summary) ? body.summary : {},
      prompt_version: typeof body.prompt_version === 'string' ? body.prompt_version : '',
      total: intValueOrZero(body.total),
      items: Array.isArray(body.items) ? (body.items as Record<string, unknown>[]) : [],
    };
  }
}

// ── helpers (Go dataset_artifact_views.go 대응) ───────────────────────────────

function normalizeArtifactPagination(limit: number, offset: number): { limit: number; offset: number } {
  if (!Number.isFinite(limit) || limit <= 0) {
    limit = 100;
  }
  if (limit > 1000) {
    limit = 1000;
  }
  if (!Number.isFinite(offset) || offset < 0) {
    offset = 0;
  }
  return { limit: Math.trunc(limit), offset: Math.trunc(offset) };
}

/** Go resolveArtifactStatus — 결정 4 status 매핑 + cancelled 처리. */
function resolveArtifactStatus(
  ref: string,
  job: LatestBuildJobRow | undefined,
  metadataStatus: string,
  cancelled: boolean,
): string {
  const hasArtifact = ref.trim() !== '';
  if (cancelled && (job === undefined || (job.status !== 'running' && job.status !== 'queued'))) {
    return 'cancelled';
  }
  if (job === undefined) {
    return hasArtifact ? 'completed' : 'not_started';
  }
  switch (job.status) {
    case 'queued':
    case 'running':
    case 'failed':
      return job.status;
    case 'completed':
      return hasArtifact ? 'completed' : 'failed';
    default:
      return metadataStatus.trim() !== '' ? metadataStatus : job.status;
  }
}

/** Go computeDurationSeconds — µs 정밀도 유지 (running이면 now 기준). */
function computeDurationSeconds(started: string | null, completed: string | null): number | null {
  if (started == null) {
    return null;
  }
  const startMicros = pgEpochMicros(started);
  if (startMicros === null) {
    return null;
  }
  const endMicros =
    completed != null ? pgEpochMicros(completed) ?? Date.now() * 1000 : Date.now() * 1000;
  const diffMicros = endMicros - startMicros;
  // Go Duration.Seconds()와 동일 공식(정수초 + 나머지ns/1e9 합산) — 한 번에
  // µs/1e6으로 나누면 부동소수점 표현이 어긋난다 (2.3688 vs 2.3688000000000002).
  const sec = Math.floor(diffMicros / 1_000_000);
  const nsec = (diffMicros - sec * 1_000_000) * 1000;
  const seconds = sec + nsec / 1_000_000_000;
  return seconds < 0 ? 0 : seconds;
}

/** Go GetDocGenuinenessView/GetClauseLabelView의 cleanRef 해석 — 파일 없으면 join 생략. */
function resolveCleanRef(meta: Record<string, unknown>): string {
  let cleanRef = metadataString(meta, 'clean_uri');
  if (!cleanRef) {
    cleanRef = metadataString(meta, 'cleaned_ref');
  }
  if (cleanRef && !existsSync(rewriteWorkspacePath(cleanRef))) {
    cleanRef = '';
  }
  return cleanRef;
}

/** Go runningClauseLabelApplied — 진행 중 빌드의 이번 실행 메타(request 회수). */
function runningClauseLabelApplied(
  job: LatestBuildJobRow | undefined,
): Record<string, unknown> | null {
  if (!job || (job.status !== 'running' && job.status !== 'queued')) {
    return null;
  }
  const applied: Record<string, unknown> = {};
  const prompt = jobRequestString(job, 'clause_label_prompt_version');
  if (prompt) {
    applied['prompt_version'] = prompt;
  }
  const model = jobRequestString(job, 'model_id');
  if (model) {
    applied['model'] = model;
    const display = modelDisplayNameFor(model);
    if (display) {
      applied['model_display_name'] = display;
    }
  }
  return Object.keys(applied).length > 0 ? applied : null;
}

function jobRequestString(job: LatestBuildJobRow, key: string): string {
  const value = job.request?.[key];
  return typeof value === 'string' ? value.trim() : '';
}

/** Go GetCleanView summary — buildCleanSummary(metadata) → cleanSummaryToMap 합성. */
function cleanViewSummary(meta: Record<string, unknown>): Record<string, unknown> | null {
  const raw = meta['clean_summary'];
  if (!isPlainObject(raw) || Object.keys(raw).length === 0) {
    return null;
  }
  const result: Record<string, unknown> = {
    input_row_count: intValueOrZero(raw['input_row_count']),
    output_row_count: intValueOrZero(raw['output_row_count']),
    kept_count: intValueOrZero(raw['kept_count']),
    dropped_count: intValueOrZero(raw['dropped_count']),
    deduped_count: intValueOrZero(raw['deduped_count']),
  };
  const skipped = intValueOrZero(raw['skipped_row_count']);
  if (skipped > 0) {
    result['skipped_row_count'] = skipped;
  }
  const textColumn = anyStringValue(raw['text_column']).trim();
  if (textColumn) {
    result['text_column'] = textColumn;
  }
  const textColumns = anyStringList(raw['text_columns']);
  if (textColumns) {
    result['text_columns'] = textColumns;
  }
  const sourceChars = intValueOrZero(raw['source_input_char_count']);
  if (sourceChars > 0) {
    result['source_input_char_count'] = sourceChars;
  }
  const cleanedChars = intValueOrZero(raw['cleaned_input_char_count']);
  if (cleanedChars > 0) {
    result['cleaned_input_char_count'] = cleanedChars;
  }
  const reducedChars = intValueOrZero(raw['clean_reduced_char_count']);
  if (reducedChars > 0) {
    result['clean_reduced_char_count'] = reducedChars;
  }
  const ruleHits = intMapValue(raw['clean_regex_rule_hits']);
  if (ruleHits) {
    result['clean_regex_rule_hits'] = ruleHits;
  }
  return result;
}

function summaryMetadataString(
  meta: Record<string, unknown>,
  summaryKey: string,
  field: string,
): string {
  const summary = meta[summaryKey];
  if (!isPlainObject(summary)) {
    return '';
  }
  const value = summary[field];
  return typeof value === 'string' ? value.trim() : '';
}

function summaryMetadataValue(
  meta: Record<string, unknown>,
  summaryKey: string,
  field: string,
): { present: boolean; value: unknown } {
  const summary = meta[summaryKey];
  if (!isPlainObject(summary) || !(field in summary)) {
    return { present: false, value: undefined };
  }
  return { present: true, value: summary[field] };
}

function summaryMetadataMap(
  meta: Record<string, unknown>,
  summaryKey: string,
  field: string,
): Record<string, unknown> | null {
  const { present, value } = summaryMetadataValue(meta, summaryKey, field);
  if (!present || !isPlainObject(value)) {
    return null;
  }
  return value;
}

/** Go clauseLabelIncludedTier — clause_label build 기본 포함 집합. */
function clauseLabelIncludedTier(tier: string): boolean {
  return tier === 'genuine_review' || tier === 'uncertain';
}

/** Go applyDocGenuinenessOverrides — effective label 합성 + summary 재집계. */
function applyDocGenuinenessOverrides(
  view: DatasetArtifactViewDto,
  overrides: DocGenuinenessOverrideRow[],
): boolean {
  if (overrides.length === 0) {
    return false;
  }
  const byDoc = new Map(overrides.map((o) => [o.doc_id, o]));
  for (const item of view.items ?? []) {
    const docId = typeof item['doc_id'] === 'string' ? item['doc_id'] : '';
    const override = byDoc.get(docId);
    if (!override) {
      continue;
    }
    let originalGenuineness = typeof item['genuineness'] === 'string' ? item['genuineness'] : '';
    if (!originalGenuineness) {
      originalGenuineness = override.original_genuineness;
    }
    let originalReason = typeof item['reason'] === 'string' ? item['reason'] : '';
    if (!originalReason) {
      originalReason = override.original_reason;
    }
    item['original_genuineness'] = originalGenuineness;
    item['original_reason'] = originalReason;
    item['override_genuineness'] = override.override_genuineness;
    item['override_reason'] = override.override_reason;
    item['is_overridden'] = true;
    item['genuineness'] = override.override_genuineness;
    item['reason'] = override.override_reason;
  }

  let crossed = false;
  if (view.summary) {
    const counts = view.summary['genuineness'];
    if (isPlainObject(counts)) {
      for (const override of overrides) {
        if (override.original_genuineness !== '') {
          const current = intValueOrZero(counts[override.original_genuineness]) - 1;
          counts[override.original_genuineness] = current < 0 ? 0 : current;
        }
        counts[override.override_genuineness] =
          intValueOrZero(counts[override.override_genuineness]) + 1;
        if (
          clauseLabelIncludedTier(override.original_genuineness) !==
          clauseLabelIncludedTier(override.override_genuineness)
        ) {
          crossed = true;
        }
      }
      view.summary['genuineness'] = counts;
    }
    view.summary['override_count'] = overrides.length;
    view.summary['downstream_boundary_crossed'] = crossed;
  }
  return crossed;
}

/** Go applyClauseLabelOverrides — effective aspect/sentiment 합성 + summary 재집계. */
function applyClauseLabelOverrides(
  view: DatasetArtifactViewDto,
  overrides: ClauseLabelOverrideRow[],
): void {
  if (overrides.length === 0) {
    return;
  }
  const byClause = new Map(overrides.map((o) => [o.clause_id, o]));
  for (const item of view.items ?? []) {
    const clauseId = typeof item['clause_id'] === 'string' ? item['clause_id'] : '';
    const override = byClause.get(clauseId);
    if (!override) {
      continue;
    }
    let origAspect = typeof item['aspect'] === 'string' ? item['aspect'] : '';
    if (!origAspect) {
      origAspect = override.original_aspect;
    }
    let origSentiment = typeof item['sentiment'] === 'string' ? item['sentiment'] : '';
    if (!origSentiment) {
      origSentiment = override.original_sentiment;
    }
    item['original_aspect'] = origAspect;
    item['original_sentiment'] = origSentiment;
    item['override_aspect'] = override.override_aspect;
    item['override_sentiment'] = override.override_sentiment;
    item['override_reason'] = override.override_reason;
    item['is_overridden'] = true;
    item['aspect'] = override.override_aspect;
    item['sentiment'] = override.override_sentiment;
  }

  if (!view.summary) {
    return;
  }
  const sentimentCounts = view.summary['sentiment'];
  if (isPlainObject(sentimentCounts)) {
    for (const override of overrides) {
      adjustCountMap(sentimentCounts, override.original_sentiment, override.override_sentiment);
    }
    view.summary['sentiment'] = sentimentCounts;
  }
  const aspectCounts = view.summary['aspect'];
  if (isPlainObject(aspectCounts)) {
    for (const override of overrides) {
      adjustCountMap(aspectCounts, override.original_aspect, override.override_aspect);
    }
    view.summary['aspect'] = aspectCounts;
  }
  const aspectSentiment = view.summary['aspect_sentiment'];
  if (isPlainObject(aspectSentiment)) {
    const affected = new Set<string>();
    for (const override of overrides) {
      adjustAspectSentimentCount(aspectSentiment, override.original_aspect, override.original_sentiment, -1);
      adjustAspectSentimentCount(aspectSentiment, override.override_aspect, override.override_sentiment, +1);
      affected.add(override.original_aspect);
      affected.add(override.override_aspect);
    }
    for (const aspect of affected) {
      recomputeAspectSentimentPercents(aspectSentiment, aspect);
    }
    view.summary['aspect_sentiment'] = aspectSentiment;
  }
  view.summary['override_count'] = overrides.length;
}

function adjustCountMap(map: Record<string, unknown>, from: string, to: string): void {
  if (from !== '') {
    const next = intValueOrZero(map[from]) - 1;
    map[from] = next < 0 ? 0 : next;
  }
  if (to !== '') {
    map[to] = intValueOrZero(map[to]) + 1;
  }
}

function adjustAspectSentimentCount(
  aspectSentiment: Record<string, unknown>,
  aspect: string,
  sentiment: string,
  delta: number,
): void {
  if (aspect === '' || sentiment === '') {
    return;
  }
  let entry = aspectSentiment[aspect];
  if (!isPlainObject(entry)) {
    entry = { total: 0, sentiment: {} };
    aspectSentiment[aspect] = entry;
  }
  const entryObj = entry as Record<string, unknown>;
  let dist = entryObj['sentiment'];
  if (!isPlainObject(dist)) {
    dist = {};
    entryObj['sentiment'] = dist;
  }
  const distObj = dist as Record<string, unknown>;
  let cell = distObj[sentiment];
  if (!isPlainObject(cell)) {
    cell = { count: 0, percent: 0 };
    distObj[sentiment] = cell;
  }
  const cellObj = cell as Record<string, unknown>;
  const count = intValueOrZero(cellObj['count']) + delta;
  cellObj['count'] = count < 0 ? 0 : count;
}

function recomputeAspectSentimentPercents(
  aspectSentiment: Record<string, unknown>,
  aspect: string,
): void {
  const entry = aspectSentiment[aspect];
  if (!isPlainObject(entry)) {
    return;
  }
  const dist = entry['sentiment'];
  if (!isPlainObject(dist)) {
    return;
  }
  let total = 0;
  for (const value of Object.values(dist)) {
    if (isPlainObject(value)) {
      total += intValueOrZero(value['count']);
    }
  }
  entry['total'] = total;
  for (const value of Object.values(dist)) {
    if (isPlainObject(value)) {
      value['percent'] = percentOf(intValueOrZero(value['count']), total);
    }
  }
}

interface DocGenuinenessRun {
  model: string;
  ref: string;
  promptVersion: string;
  /** KST RFC3339 표기. 없거나 파싱 실패면 Go zero time과 동일한 표기. */
  completedAt: string;
}

const GO_ZERO_TIME = '0001-01-01T00:00:00Z';

/**
 * Go docGenuinenessRunsFromMetadata — metadata.doc_genuineness_runs 파싱.
 * runs 키가 없는 옛 버전은 단일 doc_genuineness_ref + summary.model로 run 1건 합성.
 */
function docGenuinenessRunsFromMetadata(meta: Record<string, unknown>): DocGenuinenessRun[] {
  const raw = meta['doc_genuineness_runs'];
  const runs: DocGenuinenessRun[] = [];
  if (Array.isArray(raw)) {
    for (const item of raw) {
      if (!isPlainObject(item)) {
        continue;
      }
      const model = anyStringValue(item['model']).trim();
      const ref = anyStringValue(item['ref']).trim();
      if (!model || !ref) {
        continue;
      }
      runs.push({
        model,
        ref,
        promptVersion: anyStringValue(item['prompt_version']).trim(),
        completedAt: metadataTimeValue(item['completed_at']),
      });
    }
  }
  if (runs.length > 0) {
    return runs;
  }
  // 하위 호환 — 옛 단일 결과를 run 1건으로.
  let ref = metadataString(meta, 'doc_genuineness_ref');
  if (!ref) {
    ref = metadataString(meta, 'doc_genuineness_uri');
  }
  if (!ref) {
    return [];
  }
  const summary = meta['doc_genuineness_summary'];
  let model = '';
  if (isPlainObject(summary) && typeof summary['model'] === 'string') {
    model = summary['model'].trim();
  }
  return [
    {
      model: model || 'default',
      ref,
      promptVersion: metadataString(meta, 'doc_genuineness_prompt_version'),
      completedAt: metadataTimeValue(meta['doc_genuineness_completed_at']),
    },
  ];
}

/** Go anyTimeValue + KST marshal — RFC3339 문자열 파싱, 실패 시 zero time 표기. */
function metadataTimeValue(value: unknown): string {
  if (typeof value === 'string') {
    const formatted = goRfc3339(value);
    if (formatted) {
      return formatted;
    }
  }
  return GO_ZERO_TIME;
}

/** Go percentOf — 소수 1자리 half-away-from-zero 반올림. */
function percentOf(count: number, total: number): number {
  if (total <= 0) {
    return 0;
  }
  return Math.floor((count / total) * 1000 + 0.5) / 10;
}

/**
 * Go subjectDerivedTerms — dataset.metadata.doc_genuineness의 subject_name +
 * subject_aliases + recruitment_keywords를 구분자(공백/&/·/구두점)로 쪼갠 토큰 집합
 * (소문자, 2글자 미만 제거). "추천 제외어" 매칭용.
 */
function subjectDerivedTerms(datasetMeta: Record<string, unknown>): Set<string> {
  const raw = datasetMeta['doc_genuineness'];
  const terms = new Set<string>();
  if (!isPlainObject(raw)) {
    return terms;
  }
  const parts: string[] = [anyStringValue(raw['subject_name'])];
  parts.push(...(anyStringList(raw['subject_aliases']) ?? []));
  parts.push(...(anyStringList(raw['recruitment_keywords']) ?? []));
  for (const part of parts) {
    for (const token of part.split(/[\s&/,·・|+()[\]\-_~]+/u)) {
      const term = token.trim().toLowerCase();
      if ([...term].length >= 2) {
        terms.add(term);
      }
    }
  }
  return terms;
}

/** Go annotateSuggestedExclude — 현재 페이지의 keyword 행에 검색어 유래 플래그. */
function annotateSuggestedExclude(
  items: Record<string, unknown>[],
  terms: Set<string>,
): number {
  if (terms.size === 0) {
    return 0;
  }
  let count = 0;
  for (const item of items) {
    const keyword = item['keyword'];
    if (typeof keyword !== 'string') {
      continue;
    }
    if (terms.has(keyword.trim().toLowerCase())) {
      item['suggested_exclude'] = true;
      count++;
    }
  }
  return count;
}
