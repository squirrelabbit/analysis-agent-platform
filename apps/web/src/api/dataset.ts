import { apiClient } from './client'
import type { CreateDatasetPayload, DatasetListResponse, DatasetResponse, DatasetVersionResponse } from '@/types/dto/dataset.dto'

export const datasetsApi = {
  getAll: (projectId: string) =>
    apiClient
      .get<DatasetListResponse>(`/projects/${projectId}/datasets`)
      .then((r) => r.data.items),

  create: (projectId: string, payload: CreateDatasetPayload) =>
      apiClient.post<DatasetResponse>(`/projects/${projectId}/datasets`, payload).then((r) => r.data),

  getById: (projectId: string, datasetId: string) => 
    apiClient.get<DatasetResponse>(`/projects/${projectId}/datasets/${datasetId}`).then(r => r.data),

  getVersionsByDataset: (projectId: string, datasetId: string) => 
    apiClient.get<{items: DatasetVersionResponse[]}>(`/projects/${projectId}/datasets/${datasetId}/versions`).then(r => r.data.items)

}
