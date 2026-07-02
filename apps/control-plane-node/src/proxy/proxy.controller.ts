import { Controller, Get, HttpException, Query } from '@nestjs/common';
import { httpError } from '../common/errors';
import { lloaModelOptions } from '../common/lloa-models';
import { PythonWorkerClient } from '../worker/worker-client';

/**
 * worker proxy read 4종 — Go handlePromptOptions/handleTaxonomy/handleTaxonomies/
 * handleLLOAModelOptions 계약 동일. control-plane은 prompt/taxonomy 파일을 직접
 * 읽지 않고 worker 응답 JSON을 그대로 전달한다. worker 4xx는 {"error"} 메시지를
 * 400 {detail}로, 5xx/연결 오류는 500으로 surface (Go writeServiceError 매핑).
 */
@Controller()
export class ProxyController {
  constructor(private readonly worker: PythonWorkerClient) {}

  /** GET /prompt_options?task=<task> — worker /tasks/prompt_options proxy. */
  @Get('prompt_options')
  async promptOptions(@Query('task') task?: string): Promise<unknown> {
    const trimmed = (task ?? '').trim();
    if (!trimmed) {
      throw httpError(400, 'task query parameter is required');
    }
    return this.proxyTask('prompt_options', { task: trimmed });
  }

  /** GET /taxonomy?taxonomy_id=<id> — worker /tasks/taxonomy proxy (미지정 시 worker default). */
  @Get('taxonomy')
  async taxonomy(@Query('taxonomy_id') taxonomyId?: string): Promise<unknown> {
    return this.proxyTask('taxonomy', { taxonomy_id: (taxonomyId ?? '').trim() });
  }

  /** GET /taxonomies — worker /tasks/taxonomies proxy ({items, default}). */
  @Get('taxonomies')
  async taxonomies(): Promise<unknown> {
    return this.proxyTask('taxonomies', {});
  }

  /** GET /lloa_model_options — config/lloa_models.json allowlist ({items: []}). */
  @Get('lloa_model_options')
  lloaModelOptions(): { items: unknown[] } {
    return { items: lloaModelOptions() };
  }

  private async proxyTask(task: string, payload: Record<string, unknown>): Promise<unknown> {
    let result: { status: number; body: unknown; text: string };
    try {
      result = await this.worker.postTask(task, payload);
    } catch (error) {
      throw new HttpException({ detail: `${task} worker call: ${String(error)}` }, 500);
    }
    if (result.status >= 400 && result.status < 500) {
      throw httpError(400, workerErrorMessage(result.body, result.text));
    }
    if (result.status >= 500) {
      throw new HttpException(
        { detail: `${task} worker returned ${result.status}: ${result.text}` },
        500,
      );
    }
    return result.body;
  }
}

/** Go promptOptionsErrorMessage — {"error": "..."} 추출, 실패 시 raw body. */
function workerErrorMessage(body: unknown, text: string): string {
  if (typeof body === 'object' && body !== null) {
    const message = (body as Record<string, unknown>)['error'];
    if (typeof message === 'string' && message.trim()) {
      return message;
    }
  }
  return text.trim();
}
