export const reportKeys = {
  all: ["reports"] as const,

  savedResults: () => [...reportKeys.all, "savedResults"] as const,
  // dataset_id 필터가 다르면 별도 캐시. 미지정(project 전체)은 null로 구분.
  savedResultList: (projectId: string, datasetId?: string) =>
    [...reportKeys.savedResults(), "list", projectId, datasetId ?? null] as const,
};
