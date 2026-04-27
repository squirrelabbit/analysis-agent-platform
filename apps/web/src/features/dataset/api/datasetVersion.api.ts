import { apiClient } from "@/api/client";
import {
  type PreparePreviewResponse,
  type DatasetVersionListResponse,
  type DatasetVersionResponse,
  type UploadDatasetVersionRequest,
  type SentimentPreviewResponse,
} from "../types/datasetVersion.dto";

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
      .get<DatasetVersionResponse>(
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

  downloadSourceFile: (
    projectId: string,
    datasetId: string,
    versionId: string,
  ) =>
    apiClient
      .get<void>(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/source_download`,
      )
      .then((r) => r.data),

  downloadCleanFile: (
    projectId: string,
    datasetId: string,
    versionId: string,
  ) =>
    apiClient
      .get<void>(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/clean_download`,
      )
      .then((r) => r.data),

  runDatasetVersion: (
    projectId: string,
    datasetId: string,
    versionId: string,
    type: "prepare" | "sentiment",
  ) =>
    apiClient
      .post<void>(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}`,
      )
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

  getPreparePreview: (
    projectId: string,
    datasetId: string,
    versionId: string,
    // limit?: number,
  ) =>
    apiClient.get<PreparePreviewResponse>(
      `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/prepare_preview`,
    ),

  downloadPreparePreview: (
    projectId: string,
    datasetId: string,
    versionId: string,
  ) =>
    apiClient
      .get<void>(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/prepare_download`,
      )
      .then((r) => r.data),

  getSentimentPreview: (
    projectId: string,
    datasetId: string,
    versionId: string,
    // limit?: number,
  ) =>
    apiClient.get<SentimentPreviewResponse>(
      `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/prepare_preview`,
    ),

  downloadSentimentPreview: (
    projectId: string,
    datasetId: string,
    versionId: string,
  ) =>
    apiClient
      .get<void>(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/sentiment_download`,
      )
      .then((r) => r.data),
};
