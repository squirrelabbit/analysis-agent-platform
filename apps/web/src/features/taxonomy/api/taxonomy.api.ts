import { apiClient } from "@/api/client";
import type { TaxonomyResponseDto } from "../models";

// 사용 가능한 taxonomy 목록(요약) — 선택 UI용. GET /taxonomies.
export interface TaxonomyListItemDto {
  taxonomy_id: string;
  domain: string;
  aspect_count: number;
  taxonomy_hash?: string;
  is_default: boolean;
}
export interface TaxonomyListResponseDto {
  items: TaxonomyListItemDto[];
  default: string;
}

export const taxonomyApi = {
  // taxonomyId 미지정 시 백엔드 default.
  get: (taxonomyId?: string) =>
    apiClient
      .get<TaxonomyResponseDto>("/taxonomy", {
        params: taxonomyId ? { taxonomy_id: taxonomyId } : undefined,
      })
      .then((r) => r.data),
  list: () =>
    apiClient.get<TaxonomyListResponseDto>("/taxonomies").then((r) => r.data),
};
