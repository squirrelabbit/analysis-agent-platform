import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

/**
 * Go lloa_model_registry.go + modelDisplayNameFor 포팅 — 전처리 빌드 모델 allowlist
 * (config/lloa_models.json, LLOA_MODELS_PATH env override)와 화면 표시명.
 * 표시명은 catalog 라벨 우선, 없으면 env 단일쌍(LLOA_MODEL/LLOA_MODEL_DISPLAY_NAME)
 * fallback. 어디에도 없으면 '' → 미노출(프론트가 raw model로 fallback).
 */

export interface LLOAModelOption {
  model_id: string;
  label: string;
  default: boolean;
}

let cachedOptions: LLOAModelOption[] | null = null;

/** GET /lloa_model_options 응답용 — catalog.default 일치 항목(없으면 첫 항목)이 default. */
export function lloaModelOptions(): LLOAModelOption[] {
  if (cachedOptions !== null) {
    return cachedOptions;
  }
  const path =
    process.env.LLOA_MODELS_PATH ?? resolve(process.cwd(), '..', '..', 'config', 'lloa_models.json');
  try {
    const catalog = JSON.parse(readFileSync(path, 'utf-8'));
    const seen = new Set<string>();
    const options: LLOAModelOption[] = [];
    for (const model of Array.isArray(catalog?.models) ? catalog.models : []) {
      const id = typeof model?.model_id === 'string' ? model.model_id.trim() : '';
      if (!id || seen.has(id)) {
        continue;
      }
      seen.add(id);
      const label = typeof model?.label === 'string' && model.label.trim() ? model.label.trim() : id;
      options.push({ model_id: id, label, default: false });
    }
    if (options.length > 0) {
      const defaultModel = typeof catalog?.default === 'string' ? catalog.default.trim() : '';
      const defaultIndex = Math.max(
        options.findIndex((option) => option.model_id === defaultModel),
        0,
      );
      options[defaultIndex].default = true;
    }
    cachedOptions = options;
  } catch {
    cachedOptions = []; // 파일 부재는 정상(Go도 빈 목록) — env fallback만 동작.
  }
  return cachedOptions;
}

export function modelDisplayNameFor(model: string): string {
  const trimmed = model.trim();
  if (!trimmed) {
    return '';
  }
  for (const option of lloaModelOptions()) {
    if (option.model_id === trimmed && option.label !== option.model_id) {
      return option.label;
    }
  }
  const envDisplay = (process.env.LLOA_MODEL_DISPLAY_NAME ?? '').trim();
  const envModel = (process.env.LLOA_MODEL ?? '').trim();
  if (!envDisplay || trimmed !== envModel) {
    return '';
  }
  return envDisplay;
}
