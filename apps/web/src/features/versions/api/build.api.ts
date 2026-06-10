import { apiClient } from "@/api/client";
import type { BuildJobType } from "@/shared/types/common";

export interface BuildViewParams {
  limit?: number;
  offset?: number;
  aspect?: string;
  sentiment?: string;
  genuineness?: string;
  // 키워드/절 부분일치 검색 (clause_keywords 등).
  q?: string;
}

export const buildApi = {
  getBuildVersion: (
    projectId: string,
    datasetId: string,
    versionId: string,
    type: BuildJobType,
    params?: BuildViewParams,
  ) =>
    apiClient
      .get(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}`,
        { params },
      )
      .then(({ data }) => data),

  runBuildVersion: <T>(
    projectId: string,
    datasetId: string,
    versionId: string,
    type: BuildJobType,
    req?: T
  ) =>
    apiClient
      .post(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}`, req,
      )
      .then(({ data }) => data),
};
