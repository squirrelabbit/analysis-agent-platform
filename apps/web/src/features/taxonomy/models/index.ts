import type { Taxonomy } from "./model";

export type { TaxonomyAspectDto, TaxonomyResponseDto } from "./dto";
export type { Taxonomy, TaxonomyAspect } from "./model";
export { mapTaxonomy } from "./mapper";

// aspect key를 한글 label로. taxonomy 미로딩/매칭 실패 시 key를 그대로 반환
// (백엔드 endpoint 실패해도 화면은 영문 key로 동작 — graceful fallback).
export const aspectLabelOf = (
  taxonomy: Taxonomy | undefined,
  key: string,
): string => taxonomy?.aspectLabels[key] ?? key;
