import { readFileSync } from 'node:fs';
import { goRfc3339 } from './go-time';
import { metadataString } from './metadata';
import { rewriteWorkspacePath } from './workspace-path';

/**
 * Go loadBuildJobProgress의 파일 읽기+decode 부분 — build job diagnostics와
 * artifact view가 공유한다. DTO shape은 소비처마다 달라(diagnostics는
 * elapsed_seconds 포함, view는 미포함 + omitempty 규칙 상이) raw 값을 돌려준다.
 */
export interface RawBuildProgress {
  percent: number;
  processed_rows: number;
  total_rows: number;
  elapsed_seconds: number;
  /** null = 파일에 없음/JSON null (Go *float64 nil). */
  eta_seconds: number | null;
  /** trim 적용된 메시지 ('' = 없음). */
  message: string;
  /** KST RFC3339 표기, null = 없음/파싱 실패. */
  updated_at: string | null;
}

/** metadata `<prefix>_progress_ref` 파일을 읽는다. 어떤 실패든 null (Go와 동일). */
export function loadBuildProgress(
  metadata: Record<string, unknown>,
  prefix: string,
): RawBuildProgress | null {
  const ref = metadataString(metadata, `${prefix}_progress_ref`);
  if (!ref) {
    return null;
  }
  let decoded: Record<string, unknown>;
  try {
    decoded = JSON.parse(readFileSync(rewriteWorkspacePath(ref), 'utf-8'));
  } catch {
    return null;
  }
  const processedRows = decoded.processed_rows ?? 0;
  const totalRows = decoded.total_rows ?? 0;
  // Go는 int 필드에 소수 JSON이 오면 unmarshal 실패 → progress 전체 생략.
  if (!Number.isInteger(processedRows) || !Number.isInteger(totalRows)) {
    return null;
  }
  const progress: RawBuildProgress = {
    percent: typeof decoded.percent === 'number' ? decoded.percent : 0,
    processed_rows: processedRows as number,
    total_rows: totalRows as number,
    elapsed_seconds: typeof decoded.elapsed_seconds === 'number' ? decoded.elapsed_seconds : 0,
    eta_seconds: typeof decoded.eta_seconds === 'number' ? decoded.eta_seconds : null,
    message: typeof decoded.message === 'string' ? decoded.message.trim() : '',
    updated_at: null,
  };
  if (typeof decoded.updated_at === 'string') {
    progress.updated_at = goRfc3339(decoded.updated_at);
  }
  return progress;
}
