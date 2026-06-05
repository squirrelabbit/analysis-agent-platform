import { apiClient } from "@/api/client";
import type { CreateDatasetRequest, DatasetListResponse, DatasetResponse, MetadataRequest } from "../models";


export const datasetApi = {
  getDatasets: (projectId: string) =>
    apiClient
      .get<DatasetListResponse>(`/projects/${projectId}/datasets`)
      .then((r) => r.data.items),

  getDataset: (projectId: string, datasetId: string) =>
    apiClient
      .get<DatasetResponse>(`/projects/${projectId}/datasets/${datasetId}`)
      .then((r) => r.data),

  createDataset: (projectId: string, req: CreateDatasetRequest) =>
    apiClient
      .post<DatasetResponse>(`/projects/${projectId}/datasets`, req)
      .then((r) => r.data),

  patchMetadata: (projectId: string, datasetId: string, req: MetadataRequest) =>
    apiClient
      .patch<DatasetResponse>(`/projects/${projectId}/datasets/${datasetId}/metadata`, req)
      .then((r) => r.data),

  // 이름/설명 수정 (PATCH /datasets/{id}). 지정한 필드만 반영.
  updateInfo: (
    projectId: string,
    datasetId: string,
    req: { name?: string; description?: string },
  ) =>
    apiClient
      .patch<DatasetResponse>(`/projects/${projectId}/datasets/${datasetId}`, req)
      .then((r) => r.data),

  deleteDataset: (projectId: string,  datasetId: string) =>
    apiClient
      .delete<void>(`/projects/${projectId}/datasets/${datasetId}`)
      .then((r) => r.data),
};
