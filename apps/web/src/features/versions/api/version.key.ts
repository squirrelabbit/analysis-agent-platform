import type { BuildJobType } from "@/shared/types/common";

export const versionKeys = {
  all: ["versions"] as const,

  lists: () => [...versionKeys.all, "list"] as const,

  details: () => [...versionKeys.all, "detail"] as const,

  detail: (versionId: string) =>
    [...versionKeys.details(), versionId] as const,
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