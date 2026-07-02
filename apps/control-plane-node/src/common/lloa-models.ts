import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';

/**
 * Go lloa_model_registry.go + modelDisplayNameFor 포팅 — artifact의 raw 모델 id에
 * 대한 화면 표시명. config/lloa_models.json(LLOA_MODELS_PATH env override) allowlist
 * 라벨 우선, 없으면 env 단일쌍(LLOA_MODEL/LLOA_MODEL_DISPLAY_NAME) 매칭 fallback.
 * 어디에도 없으면 '' → 미노출(프론트가 raw model로 fallback).
 */

interface LLOAModelOption {
  model_id: string;
  label: string;
}

let cachedOptions: LLOAModelOption[] | null = null;

function loadOptions(): LLOAModelOption[] {
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
      options.push({ model_id: id, label });
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
  for (const option of loadOptions()) {
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
