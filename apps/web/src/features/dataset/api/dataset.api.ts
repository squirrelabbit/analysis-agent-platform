import { apiClient } from "@/api/client";
import type {
  CreateDatasetRequest,
  DatasetListResponse,
  DatasetResponse,
} from "../types/dataset.dto";

export const datasetsApi = {
  getDatasets: (projectId: string) =>
    apiClient
      .get<DatasetListResponse>(`/projects/${projectId}/datasets`)
      .then((r) => r.data.items),

  getDatasetById: (projectId: string, datasetId: string) =>
    apiClient
      .get<DatasetResponse>(`/projects/${projectId}/datasets/${datasetId}`)
      .then((r) => r.data),

  createDataset: (projectId: string, req: CreateDatasetRequest) =>
    apiClient
      .post<DatasetResponse>(`/projects/${projectId}/datasets`, req)
      .then((r) => r.data),

  deleteDataset: (projectId: string) =>
    apiClient
      .delete<void>(`/projects/${projectId}/datasets`)
      .then((r) => r.data),
};
