import { apiClient } from "@/api/client";
import type { VersionDetailResponse, VersionListResponse, VersionResponse } from "../models/version";
import type { BuildJobType } from "@/shared/types/common";

export const versionApi = {
  getVersions: (projectId: string, datasetId: string) =>
    apiClient
      .get<VersionListResponse>(
        `/projects/${projectId}/datasets/${datasetId}/versions`,
      )
      .then((r) => r.data.items),

  getVersion: (projectId: string, datasetId: string, versionId: string) =>
    apiClient
      .get<VersionDetailResponse>(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}`,
      )
      .then(({ data }) => data),

  createVersion: (projectId: string, datasetId: string, req: FormData) =>
    apiClient.post<VersionResponse>(`/projects/${projectId}/datasets/${datasetId}/uploads`, req).then(({ data }) => data),

  activeVersion: (projectId: string, datasetId: string, versionId: string) =>
    apiClient
      .put(`/projects/${projectId}/datasets/${datasetId}/active_version`, {
        dataset_version_id: versionId,
      })
      .then(({ data }) => data),

  deleteVersion: (projectId: string, datasetId: string, versionId: string) =>
    apiClient
      .delete(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}`,
      )
      .then(({ data }) => data),

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
};
