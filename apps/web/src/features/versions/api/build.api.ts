import { apiClient } from "@/api/client";
import type { BuildJobType } from "@/shared/types/common";

export const buildApi = {
  getBuildVersion: (
    projectId: string,
    datasetId: string,
    versionId: string,
    type: BuildJobType,
  ) =>
    apiClient
      .get(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}`,
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
