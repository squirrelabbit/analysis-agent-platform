import { Injectable, Logger } from '@nestjs/common';

/**
 * Python AI worker HTTP client — Go internal/skills.PythonBuildClient의 read-only 대응.
 * strangler 단계에서 Node가 처음 쓰는 worker 경로는 source_summary(ADR-024:
 * 파일 스캔·집계는 worker) 하나다. build/analyze 프록시는 후속 포팅에서 확장.
 */
@Injectable()
export class PythonWorkerClient {
  private readonly logger = new Logger(PythonWorkerClient.name);
  private readonly baseUrl =
    process.env.PYTHON_AI_WORKER_URL ?? 'http://127.0.0.1:18090';

  /**
   * POST /tasks/artifact_doc_genuineness_view · artifact_clause_label_view —
   * artifact 파일 집계(summary/items/total). Go load*Artifact(DuckDB)의 대응이라
   * 실패는 Go와 동일하게 조회 실패(500)다 — 호출측이 throw를 그대로 전파한다.
   */
  async artifactView(
    task:
      | 'artifact_doc_genuineness_view'
      | 'artifact_clause_label_view'
      | 'artifact_clause_keywords_view',
    payload: Record<string, unknown>,
  ): Promise<Record<string, unknown>> {
    const response = await fetch(`${this.baseUrl}/tasks/${task}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const body: unknown = await response.json().catch(() => null);
    if (!response.ok) {
      const detail =
        typeof (body as Record<string, unknown>)?.error === 'string'
          ? String((body as Record<string, unknown>).error)
          : `worker ${task} failed: HTTP ${response.status}`;
      throw new Error(detail);
    }
    if (typeof body !== 'object' || body === null) {
      throw new Error(`worker ${task} returned non-object response`);
    }
    return body as Record<string, unknown>;
  }

  /**
   * POST /tasks/source_summary — source 파일 프리뷰(row_count/columns).
   * Go loadDatasetSourceSummary는 in-process라 실패해도 status=error summary를
   * 돌려준다. worker 호출 자체가 실패하면(연결 불가 등) 같은 의미로 null을
   * 반환하고 warning을 남긴다 — 호출측은 프리뷰 없이(0/[]) 응답한다.
   */
  async sourceSummary(
    storageUri: string,
    sampleLimit: number,
  ): Promise<Record<string, unknown> | null> {
    try {
      const response = await fetch(`${this.baseUrl}/tasks/source_summary`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ storage_uri: storageUri, sample_limit: sampleLimit }),
      });
      if (!response.ok) {
        this.logger.warn(
          `source_summary worker call failed: HTTP ${response.status} (${storageUri})`,
        );
        return null;
      }
      const body: unknown = await response.json();
      return typeof body === 'object' && body !== null
        ? (body as Record<string, unknown>)
        : null;
    } catch (error) {
      this.logger.warn(`source_summary worker call failed: ${String(error)} (${storageUri})`);
      return null;
    }
  }
}
