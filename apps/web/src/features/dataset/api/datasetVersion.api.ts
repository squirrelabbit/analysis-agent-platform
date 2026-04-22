import { apiClient } from "@/api/client";
import {
  type PreparePreviewResponse,
  type CreateDatasetVersionRequest,
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

  createDatasetVersion: (
    projectId: string,
    datasetId: string,
    req: CreateDatasetVersionRequest,
  ) =>
    apiClient
      .post<DatasetVersionResponse>(
        `/projects/${projectId}/datasets/${datasetId}/versions`,
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

  downloadDatasetVersion: (
    projectId: string,
    datasetId: string,
    versionId: string,
  ) =>
    apiClient
      .get<void>(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/source_download`,
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
