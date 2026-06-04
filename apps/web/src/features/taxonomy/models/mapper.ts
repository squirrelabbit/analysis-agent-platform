import type { TaxonomyResponseDto } from "./dto";
import type { Taxonomy } from "./model";

export const mapTaxonomy = (dto: TaxonomyResponseDto): Taxonomy => {
  const aspects = dto.aspects.map((a) => ({
    key: a.key,
    // label이 비면 key로 fallback — 화면에 빈 칸이 노출되지 않게.
    label: a.label || a.key,
    description: a.description,
  }));
  const aspectLabels = aspects.reduce<Record<string, string>>((acc, a) => {
    acc[a.key] = a.label;
    return acc;
  }, {});
  return {
    taxonomyId: dto.taxonomy_id,
    domain: dto.domain,
    aspects,
    sentiments: dto.sentiments,
    fallbackAspect: dto.fallback_aspect,
    taxonomyHash: dto.taxonomy_hash,
    aspectLabels,
  };
};
