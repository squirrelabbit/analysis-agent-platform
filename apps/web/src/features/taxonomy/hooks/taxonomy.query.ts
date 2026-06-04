import { useQuery } from "@tanstack/react-query";
import { taxonomyApi } from "../api/taxonomy.api";
import { taxonomyKeys } from "../api/taxonomy.key";
import { mapTaxonomy } from "../models";

// taxonomy 정의는 config 기반이라 거의 변하지 않으므로 stale을 길게 둔다.
const STALE_MS = 5 * 60 * 1000;

export const useTaxonomy = (taxonomyId?: string) =>
  useQuery({
    queryKey: taxonomyKeys.detail(taxonomyId),
    queryFn: () => taxonomyApi.get(taxonomyId),
    select: mapTaxonomy,
    staleTime: STALE_MS,
    // 조회 실패해도 화면은 영문 key로 동작하므로 재시도 1회만.
    retry: 1,
  });
