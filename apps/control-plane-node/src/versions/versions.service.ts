import { Injectable } from '@nestjs/common';
import { notFound } from '../common/errors';
import { goRfc3339, goTimestamptz } from '../common/go-time';
import {
  anyStringList,
  anyStringValue,
  anyToInt,
  intMapValue,
  intValueOrZero,
  isPlainObject,
  metadataNestedString,
  metadataString,
} from '../common/metadata';
import { PythonWorkerClient } from '../worker/worker-client';
import {
  normalizeClauseLabelSummary,
  normalizeDocGenuinenessSummary,
} from './summary-normalize';
import {
  DatasetVersionDetailDto,
  DatasetVersionListItemDto,
  DatasetVersionListResponse,
  DatasetVersionStageDetailDto,
} from './version.dto';
import {
  DatasetActiveRow,
  DatasetVersionRow,
  VersionsRepository,
} from './versions.repository';

/** metadata source_summary 캐시/worker 응답에서 쓰는 조각 (Go DatasetSourceSummary). */
interface SourceSummaryLike {
  row_count?: unknown;
  column_count?: unknown;
  columns?: unknown;
}

@Injectable()
export class VersionsService {
  constructor(
    private readonly repo: VersionsRepository,
    private readonly worker: PythonWorkerClient,
  ) {}

  async list(projectId: string, datasetId: string): Promise<DatasetVersionListResponse> {
    const dataset = await this.repo.getDataset(projectId, datasetId);
    if (dataset === undefined) {
      throw notFound('dataset');
    }
    const rows = await this.repo.list(projectId, datasetId);
    const numberById = numberDatasetVersions(rows);
    const items = await Promise.all(
      rows.map((row) => this.toListItem(row, dataset, numberById[row.dataset_version_id] ?? 0)),
    );
    return { items };
  }

  async detail(
    projectId: string,
    datasetId: string,
    versionId: string,
  ): Promise<DatasetVersionDetailDto> {
    const dataset = await this.repo.getDataset(projectId, datasetId);
    if (dataset === undefined) {
      throw notFound('dataset');
    }
    const row = await this.repo.get(projectId, versionId);
    if (row === undefined || row.dataset_id !== datasetId) {
      throw notFound('dataset version');
    }
    const meta = row.metadata ?? {};

    // stored 번호 우선, legacy(미저장)면 sibling들과 created_at ASC rank.
    let versionNumber = storedVersionNumber(meta);
    if (versionNumber === 0) {
      const siblings = await this.repo.list(projectId, datasetId);
      versionNumber = numberDatasetVersions(siblings)[versionId] ?? 0;
    }

    const detail: DatasetVersionDetailDto = {
      dataset_version_id: row.dataset_version_id,
      version_number: versionNumber,
      created_at: goTimestamptz(row.created_at),
      ...(row.ready_at != null ? { ready_at: goTimestamptz(row.ready_at) } : {}),
      is_active: isActive(row, dataset),
      row_count: 0,
      column_count: 0,
      columns: [],
      byte_size: uploadByteSize(meta),
      clean: this.cleanStageDetail(row, meta),
      doc_genuineness: this.metadataStageDetail(meta, 'doc_genuineness', normalizeDocGenuinenessSummary),
      clause_label: this.metadataStageDetail(meta, 'clause_label', normalizeClauseLabelSummary),
    };

    const summary = await this.sourceSummary(meta, row.storage_uri);
    if (summary) {
      const rowCount = anyToInt(summary.row_count);
      if (rowCount !== undefined) {
        detail.row_count = rowCount;
      }
      detail.column_count = intValueOrZero(summary.column_count);
      detail.columns = summaryColumnNames(summary);
    }
    return detail;
  }

  private async toListItem(
    row: DatasetVersionRow,
    dataset: DatasetActiveRow,
    versionNumber: number,
  ): Promise<DatasetVersionListItemDto> {
    const meta = row.metadata ?? {};
    const item: DatasetVersionListItemDto = {
      dataset_version_id: row.dataset_version_id,
      version_number: versionNumber,
      created_at: goTimestamptz(row.created_at),
      is_active: isActive(row, dataset),
      row_count: 0,
      column_count: 0,
      columns: [],
      byte_size: uploadByteSize(meta),
      clean_status: cleanStatus(row, meta),
      doc_genuineness_status: metadataString(meta, 'doc_genuineness_status'),
      clause_label_status: metadataString(meta, 'clause_label_status'),
      original_filename:
        metadataNestedString(meta, 'upload', 'original_filename') ||
        metadataNestedString(meta, 'upload', 'stored_filename'),
    };
    const summary = await this.sourceSummary(meta, row.storage_uri);
    if (summary) {
      const rowCount = anyToInt(summary.row_count);
      if (rowCount !== undefined) {
        item.row_count = rowCount;
      }
      item.column_count = intValueOrZero(summary.column_count);
      item.columns = summaryColumnNames(summary);
    }
    return item;
  }

  /**
   * source 프리뷰 — metadata 캐시(source_summary, 2026-06-26 이후 생성분) 우선,
   * 없으면(legacy) Python worker source_summary task로 계산 (Go는 in-process
   * DuckDB fallback — ADR-024에 따라 Node는 worker 위임).
   */
  private async sourceSummary(
    meta: Record<string, unknown>,
    storageUri: string,
  ): Promise<SourceSummaryLike | null> {
    const cached = meta['source_summary'];
    if (isPlainObject(cached)) {
      return cached as SourceSummaryLike;
    }
    return (await this.worker.sourceSummary(storageUri, 0)) as SourceSummaryLike | null;
  }

  /** clean stage — status는 cleanStatus 규칙, completed_at은 metadata.cleaned_at 우선. */
  private cleanStageDetail(
    row: DatasetVersionRow,
    meta: Record<string, unknown>,
  ): DatasetVersionStageDetailDto {
    const stage: DatasetVersionStageDetailDto = { status: cleanStatus(row, meta) };
    const completedAt = cleanedAtValue(row, meta);
    if (completedAt) {
      stage.completed_at = completedAt;
    }
    const summary = buildCleanSummary(meta);
    if (summary) {
      stage.summary = summary;
    }
    return stage;
  }

  /** doc_genuineness / clause_label stage — metadata `<prefix>_status/_completed_at/_summary`. */
  private metadataStageDetail(
    meta: Record<string, unknown>,
    prefix: string,
    normalize: (raw: unknown) => unknown,
  ): DatasetVersionStageDetailDto {
    const stage: DatasetVersionStageDetailDto = {
      status: metadataString(meta, `${prefix}_status`),
    };
    const completedAt = metadataTimeKst(meta, `${prefix}_completed_at`);
    if (completedAt) {
      stage.completed_at = completedAt;
    }
    if (`${prefix}_summary` in meta) {
      const normalized = normalize(meta[`${prefix}_summary`]);
      if (normalized != null) {
        stage.summary = normalized;
      }
    }
    return stage;
  }

}

function isActive(row: DatasetVersionRow, dataset: DatasetActiveRow): boolean {
  return (
    dataset.active_dataset_version_id != null &&
    dataset.active_dataset_version_id === row.dataset_version_id
  );
}

/** Go cleanStatus — metadata.clean_status → 컬럼 clean_status → data_type 기본값. */
function cleanStatus(row: DatasetVersionRow, meta: Record<string, unknown>): string {
  const fromMeta = metadataString(meta, 'clean_status');
  if (fromMeta) {
    return fromMeta;
  }
  const fromColumn = (row.clean_status ?? '').trim();
  if (fromColumn) {
    return fromColumn;
  }
  switch (row.data_type) {
    case 'unstructured':
    case 'mixed':
    case 'both':
      return 'not_requested';
    default:
      return 'not_applicable';
  }
}

/**
 * cleaned_at — **컬럼 우선**. Go store normalizeDatasetVersionCleanFields가 scan 직후
 * 컬럼값(non-null이면)을 metadata["cleaned_at"]에 time.Time으로 덮어쓰므로, 이후
 * service enrich의 metadata 우선 로직이 실질적으로 컬럼값을 본다. 컬럼이 NULL일 때만
 * metadata 문자열(RFC3339) 파싱.
 */
function cleanedAtValue(row: DatasetVersionRow, meta: Record<string, unknown>): string | null {
  if (row.cleaned_at != null) {
    return goTimestamptz(row.cleaned_at);
  }
  return metadataTimeKst(meta, 'cleaned_at');
}

/** Go metadataTime — string 값을 RFC3339로 파싱 성공 시에만 (KST 표기로) 반환. */
function metadataTimeKst(meta: Record<string, unknown>, key: string): string | null {
  const value = meta[key];
  if (typeof value !== 'string') {
    return null;
  }
  return goRfc3339(value);
}

/** Go summarize*의 byte_size — metadata.upload.byte_size 숫자만. */
function uploadByteSize(meta: Record<string, unknown>): number {
  const upload = meta['upload'];
  if (!isPlainObject(upload)) {
    return 0;
  }
  return intValueOrZero(upload['byte_size']);
}

function summaryColumnNames(summary: SourceSummaryLike): string[] {
  if (!Array.isArray(summary.columns)) {
    return [];
  }
  const names: string[] = [];
  for (const column of summary.columns) {
    if (isPlainObject(column) && typeof column.name === 'string') {
      names.push(column.name);
    }
  }
  return names;
}

/** Go storedVersionNumber — metadata.version_number(int/float) 없으면 0. */
function storedVersionNumber(meta: Record<string, unknown>): number {
  return intValueOrZero(meta['version_number']);
}

/**
 * Go numberDatasetVersions — created_at ASC(동률은 id ASC) 정렬 기준 생성순 번호.
 * metadata 저장값 우선, 없으면(legacy) rank fallback. created_at은 세션 offset이
 * 고정된 pg text라 문자열 비교가 시간 순서와 일치한다.
 */
function numberDatasetVersions(rows: DatasetVersionRow[]): Record<string, number> {
  const asc = [...rows].sort((a, b) => {
    if (a.created_at === b.created_at) {
      return a.dataset_version_id < b.dataset_version_id ? -1 : 1;
    }
    return a.created_at < b.created_at ? -1 : 1;
  });
  const out: Record<string, number> = {};
  asc.forEach((row, index) => {
    const stored = storedVersionNumber(row.metadata ?? {});
    out[row.dataset_version_id] = stored !== 0 ? stored : index + 1;
  });
  return out;
}

/** Go buildCleanSummary — metadata.clean_summary → DatasetCleanSummary 계약 (omitempty 규칙 포함). */
function buildCleanSummary(meta: Record<string, unknown>): Record<string, unknown> | null {
  const raw = meta['clean_summary'];
  if (!isPlainObject(raw) || Object.keys(raw).length === 0) {
    return null;
  }
  const out: Record<string, unknown> = {
    input_row_count: intValueOrZero(raw['input_row_count']),
    output_row_count: intValueOrZero(raw['output_row_count']),
    kept_count: intValueOrZero(raw['kept_count']),
    dropped_count: intValueOrZero(raw['dropped_count']),
  };
  const dedupedCount = intValueOrZero(raw['deduped_count']);
  if (dedupedCount !== 0) {
    out['deduped_count'] = dedupedCount;
  }
  const skippedRowCount = intValueOrZero(raw['skipped_row_count']);
  if (skippedRowCount !== 0) {
    out['skipped_row_count'] = skippedRowCount;
  }
  const textColumn = anyStringValue(raw['text_column']).trim();
  if (textColumn) {
    out['text_column'] = textColumn;
  }
  const textColumns = anyStringList(raw['text_columns']);
  if (textColumns) {
    out['text_columns'] = textColumns;
  }
  const textJoiner = anyStringValue(raw['text_joiner']);
  if (textJoiner) {
    out['text_joiner'] = textJoiner;
  }
  const sourceInputCharCount = intValueOrZero(raw['source_input_char_count']);
  if (sourceInputCharCount !== 0) {
    out['source_input_char_count'] = sourceInputCharCount;
  }
  const cleanedInputCharCount = intValueOrZero(raw['cleaned_input_char_count']);
  if (cleanedInputCharCount !== 0) {
    out['cleaned_input_char_count'] = cleanedInputCharCount;
  }
  const cleanReducedCharCount = intValueOrZero(raw['clean_reduced_char_count']);
  if (cleanReducedCharCount !== 0) {
    out['clean_reduced_char_count'] = cleanReducedCharCount;
  }
  const regexRuleHits = intMapValue(raw['clean_regex_rule_hits']);
  if (regexRuleHits) {
    out['clean_regex_rule_hits'] = regexRuleHits;
  }
  return out;
}
