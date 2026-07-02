/**
 * Go service/dataset_versions_summary_normalize.go 포팅 — version-detail의 stage
 * summary를 build-detail과 같은 normalized shape으로 통일. raw count key
 * (tier_counts / aspect_counts / sentiment_counts / clause_count)만 정리하고
 * 부수 필드(applied / model / prompt_version / taxonomy_id 등)는 보존.
 */

import { isPlainObject } from '../common/metadata';

/** Go normalizeDocGenuinenessSummary — tier_counts → genuineness + total. */
export function normalizeDocGenuinenessSummary(raw: unknown): unknown {
  if (!isPlainObject(raw)) {
    return raw;
  }
  const out: Record<string, unknown> = {};
  let genuineness: Record<string, unknown> | null = null;
  for (const [key, value] of Object.entries(raw)) {
    if (key === 'tier_counts') {
      if (isPlainObject(value)) {
        genuineness = normalizeCountMap(value);
      }
      continue;
    }
    out[key] = value;
  }
  if (genuineness !== null) {
    out['genuineness'] = genuineness;
  }
  if ('processed_row_count' in out) {
    const total = summaryCountToInt(out['processed_row_count']);
    if (total !== undefined) {
      out['total'] = total;
    }
  } else if (genuineness !== null) {
    const sum = sumCountMap(genuineness);
    if (sum !== undefined) {
      out['total'] = sum;
    }
  }
  return out;
}

/** Go normalizeClauseLabelSummary — aspect_counts/sentiment_counts/clause_count → aspect/sentiment/total. */
export function normalizeClauseLabelSummary(raw: unknown): unknown {
  if (!isPlainObject(raw)) {
    return raw;
  }
  const out: Record<string, unknown> = {};
  let aspect: Record<string, unknown> | null = null;
  let sentiment: Record<string, unknown> | null = null;
  let clauseCount: unknown;
  let hasClauseCount = false;
  for (const [key, value] of Object.entries(raw)) {
    if (key === 'aspect_counts') {
      if (isPlainObject(value)) {
        aspect = normalizeCountMap(value);
      }
      continue;
    }
    if (key === 'sentiment_counts') {
      if (isPlainObject(value)) {
        sentiment = normalizeCountMap(value);
      }
      continue;
    }
    if (key === 'clause_count') {
      clauseCount = value;
      hasClauseCount = true;
      continue;
    }
    out[key] = value;
  }
  if (aspect !== null) {
    out['aspect'] = aspect;
  }
  if (sentiment !== null) {
    out['sentiment'] = sentiment;
  }
  if (hasClauseCount) {
    const total = summaryCountToInt(clauseCount);
    if (total !== undefined) {
      out['total'] = total;
    }
  }
  return out;
}

function normalizeCountMap(input: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(input)) {
    const count = summaryCountToInt(value);
    out[key] = count !== undefined ? count : value;
  }
  return out;
}

function sumCountMap(map: Record<string, unknown>): number | undefined {
  let sum = 0;
  for (const value of Object.values(map)) {
    const count = summaryCountToInt(value);
    if (count === undefined) {
      return undefined;
    }
    sum += count;
  }
  return sum;
}

/** Go summaryCountToInt — jsonb 숫자만 int 절삭 변환. */
function summaryCountToInt(value: unknown): number | undefined {
  if (typeof value === 'number') {
    return Math.trunc(value);
  }
  return undefined;
}
