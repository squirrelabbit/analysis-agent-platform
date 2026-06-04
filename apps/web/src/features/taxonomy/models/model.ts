export interface TaxonomyAspect {
  key: string;
  label: string;
  description: string;
}

export interface Taxonomy {
  taxonomyId: string;
  domain: string;
  aspects: TaxonomyAspect[];
  sentiments: string[];
  fallbackAspect: string;
  taxonomyHash: string;
  // aspect key → 한글 label 빠른 조회용. label이 비면 key로 fallback.
  aspectLabels: Record<string, string>;
}
