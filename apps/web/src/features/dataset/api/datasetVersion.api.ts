import { apiClient } from "@/api/client";
import {
  type DatasetVersionDetailResponse,
  type DatasetVersionListResponse,
  type DatasetVersionResponse,
  type UploadDatasetVersionRequest,
} from "../types/datasetVersion.dto";
import type { Stage } from "../types/datasetVersion";

export const datasetVersionsApi = {
  getDatasetVersions: (projectId: string, datasetId: string) =>
    apiClient
      .get<DatasetVersionListResponse>(
        `/projects/${projectId}/datasets/${datasetId}/versions`,
      )
      .then((r) => r.data.items),

  getDatasetVersionById: (
    projectId: string,
    datasetId: string,
    versionId: string,
  ) =>
    apiClient
      .get<DatasetVersionDetailResponse>(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}`,
      )
      .then((r) => r.data),

  activeDatasetVersion: (
    projectId: string,
    datasetId: string,
    versionId: string,
  ) =>
    apiClient
      .put<void>(
        `/projects/${projectId}/datasets/${datasetId}/active_version`,
        { dataset_version_id: versionId },
      )
      .then((r) => r.data),

  deleteDatasetVersion: (
    projectId: string,
    datasetId: string,
    versionId: string,
  ) =>
    apiClient
      .delete<void>(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}`,
      )
      .then((r) => r.data),

  uploadDatasetVersion: (
    projectId: string,
    datasetId: string,
    req: UploadDatasetVersionRequest,
  ) =>
    apiClient
      .post<DatasetVersionResponse>(
        `/projects/${projectId}/datasets/${datasetId}/uploads`,
        req,
      )
      .then((r) => r.data),

  downloadVersionFile: (
    projectId: string,
    datasetId: string,
    versionId: string,
    type: "source" | "clean" | "prepare" | "sentiment",
  ) =>
    apiClient.get(
      `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}_download`,
      { responseType: "blob" },
    ),

  buildJob: <T>(
    projectId: string,
    datasetId: string,
    versionId: string,
    type: Stage,
    req?: T
  ) => apiClient.post(`/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}`, req)
      .then((r) => r.data),

  runBuildJob: (
    projectId: string,
    datasetId: string,
    versionId: string,
    type: Stage
    // type: "segment" | "clause_label" | "embedding_cluster" | "keyword_index",
  ) => apiClient.post(`/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}`)
      .then((r) => r.data),

  runDatasetVersionSample: (
    projectId: string,
    datasetId: string,
    versionId: string,
    type: "prepare" | "sentiment",
  ) =>
    apiClient
      .post<void>(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}_sample`,
      )
      .then((r) => r.data),

  getVersionPreview:  (
    projectId: string,
    datasetId: string,
    versionId: string,
    type: "prepare" | "sentiment",
    // limit?: number,
  ) =>
    apiClient.get(
      `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}_preview`,
    ),
};
