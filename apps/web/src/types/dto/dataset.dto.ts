
// POST /projects/:project_id/datasets — body
export interface CreateDatasetPayload {
  name: string
  description: string,
  data_type: string
}

// GET /projects/:project_id/datasets/:dataset_id — response data
export interface DatasetResponse {
  dataset_id: string,
  project_id: string,
  name: string,
  description: string,
  data_type: string,
  created_at: string
}


// GET /projects/:project_id/datasets — response data
export interface DatasetListResponse {
  items: DatasetResponse[]
}
