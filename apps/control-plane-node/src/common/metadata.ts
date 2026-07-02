/**
 * Go service 계층의 metadata jsonb 접근 헬퍼 포팅 (helpers.go / dataset_metadata.go).
 * jsonb 값은 JSON 타입(string/number/bool/object/array)만 온다는 전제.
 */

/** Go metadataString — 값을 문자열화(fmt %v 대응) + trim, 없거나 비면 fallback(''). */
export function metadataString(metadata: Record<string, unknown>, key: string): string {
  const value = metadata[key];
  if (value === undefined || value === null) {
    return '';
  }
  return String(value).trim();
}

/** Go metadataNestedString — metadata[key]가 객체일 때만 그 안의 field를 문자열화+trim. */
export function metadataNestedString(
  metadata: Record<string, unknown>,
  key: string,
  field: string,
): string {
  const nested = metadata[key];
  if (!isPlainObject(nested)) {
    return '';
  }
  const value = nested[field];
  if (value === undefined) {
    return '';
  }
  return String(value).trim();
}

/** Go metadataBool — bool true 또는 문자열 'true'(대소문자 무관)만 true. */
export function metadataBool(metadata: Record<string, unknown>, key: string): boolean {
  const value = metadata[key];
  if (typeof value === 'boolean') {
    return value;
  }
  if (typeof value === 'string') {
    return value.trim().toLowerCase() === 'true';
  }
  return false;
}

/** Go anyToInt — 숫자만 int로(소수 절삭), 그 외 undefined. */
export function anyToInt(value: unknown): number | undefined {
  if (typeof value === 'number') {
    return Math.trunc(value);
  }
  return undefined;
}

/** Go intValueOrZero. */
export function intValueOrZero(value: unknown): number {
  return anyToInt(value) ?? 0;
}

/** Go intMapValue — 값이 int 변환 가능한 항목만 모은 map. 비면 null. */
export function intMapValue(value: unknown): Record<string, number> | null {
  if (!isPlainObject(value) || Object.keys(value).length === 0) {
    return null;
  }
  const result: Record<string, number> = {};
  for (const [key, item] of Object.entries(value)) {
    const count = anyToInt(item);
    if (count !== undefined) {
      result[key] = count;
    }
  }
  return Object.keys(result).length > 0 ? result : null;
}

/** Go anyStringValue(단순 케이스) — string 그대로, null/undefined는 '', 그 외 문자열화. */
export function anyStringValue(value: unknown): string {
  if (value === undefined || value === null) {
    return '';
  }
  return String(value);
}

/** Go anyStringList + normalizeStringList — trim/빈값 제거/중복 제거. 비면 null. */
export function anyStringList(value: unknown): string[] | null {
  let items: string[];
  if (typeof value === 'string') {
    items = [value];
  } else if (Array.isArray(value)) {
    items = value.map((item) => anyStringValue(item));
  } else {
    return null;
  }
  const seen = new Set<string>();
  const result: string[] = [];
  for (const item of items) {
    const trimmed = item.trim();
    if (!trimmed || seen.has(trimmed)) {
      continue;
    }
    seen.add(trimmed);
    result.push(trimmed);
  }
  return result.length > 0 ? result : null;
}

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
