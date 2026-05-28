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
};