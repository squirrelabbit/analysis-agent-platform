import type { Taxonomy } from "./model";

export type { TaxonomyAspectDto, TaxonomyResponseDto } from "./dto";
export type { Taxonomy, TaxonomyAspect } from "./model";
export { mapTaxonomy } from "./mapper";

// executor 결과(plan_v2)에서 aspect 값을 담는 컬럼/필드 이름. 채팅 결과 표·
// 차트가 이 필드를 만나면 aspectLabelOf로 한글 label 변환한다.
export const ASPECT_FIELD = "aspect";

// aspect key를 한글 label로. taxonomy 미로딩/매칭 실패 시 key를 그대로 반환
// (백엔드 endpoint 실패해도 화면은 영문 key로 동작 — graceful fallback).
export const aspectLabelOf = (
  taxonomy: Taxonomy | undefined,
  key: string,
): string => taxonomy?.aspectLabels[key] ?? key;
