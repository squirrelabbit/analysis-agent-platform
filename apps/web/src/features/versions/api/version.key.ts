import type { BuildJobType } from "@/shared/types/common";

export const versionKeys = {
  all: ["versions"] as const,

  lists: () => [...versionKeys.all, "list"] as const,

  list: (projectId: string, datasetId: string) =>
    [...versionKeys.lists(), projectId, datasetId] as const,

  details: () => [...versionKeys.all, "detail"] as const,

  detail: (projectId: string, datasetId: string, versionId: string) =>
    [...versionKeys.details(), projectId, datasetId, versionId] as const,
};

export const buildKeys = {
  all: ["builds"] as const,

  build: (
    versionId: string,
    type: BuildJobType,
    jobId?: string,
  ) =>
    [...buildKeys.all, versionId, type, jobId ?? "latest"] as const,

  // 전처리 모델 선택지 (전역 — 버전과 무관).
  lloaModelOptions: () => [...buildKeys.all, "lloa_model_options"] as const,

  // 키워드 정제 사전 (silverone 2026-06-25) — dataset 단위(버전 무관).
  keywordDictionary: (projectId: string, datasetId: string) =>
    [...buildKeys.all, "keyword_dictionary", projectId, datasetId] as const,
  keywordDictionaryHistory: (projectId: string, datasetId: string) =>
    [...buildKeys.all, "keyword_dictionary_history", projectId, datasetId] as const,
};