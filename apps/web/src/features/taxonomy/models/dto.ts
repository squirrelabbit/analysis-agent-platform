// GET /taxonomy?taxonomy_id={id} 응답 — aspect/sentiment taxonomy 정의.
// 각 aspect의 key(영문, artifact 값) → label(한글 표시명) 매핑을 담는다.
// (silverone 백엔드 핸드오프 2026-06-04, MR !71)
export interface TaxonomyAspectDto {
  key: string;
  label: string;
  description: string;
}

export interface TaxonomyResponseDto {
  taxonomy_id: string;
  domain: string;
  aspects: TaxonomyAspectDto[];
  sentiments: string[];
  fallback_aspect: string;
  taxonomy_hash: string;
}
