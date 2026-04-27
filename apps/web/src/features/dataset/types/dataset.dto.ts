export interface DatasetResponse {
  dataset_id: string,
  project_id: string,
  name: string,
  description: string,
  data_type: 'structured' | 'unstructured',
  active_dataset_version_id: string,
  active_version_updated_at: string,
  created_at: string,
}

export interface DatasetListResponse {
  items: DatasetResponse[]
}

export interface CreateDatasetRequest {
  name: string,
  description: string,
  data_type: string
}
