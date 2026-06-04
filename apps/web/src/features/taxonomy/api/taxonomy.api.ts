import { apiClient } from "@/api/client";
import type { TaxonomyResponseDto } from "../models";

export const taxonomyApi = {
  // taxonomyId 미지정 시 백엔드 default(현재 festival-v2).
  get: (taxonomyId?: string) =>
    apiClient
      .get<TaxonomyResponseDto>("/taxonomy", {
        params: taxonomyId ? { taxonomy_id: taxonomyId } : undefined,
      })
      .then((r) => r.data),
};
