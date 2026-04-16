
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

export interface DatasetVersionResponse {
    dataset_version_id: string,
    dataset_id: string,
    project_id: string,
    stroage_uri: string,
    data_type: string,
    record_count: number,
    metadata: any,
    profile: Record<string, any> | null
    prepare_status: string,
    prepare_model: string,
    prepare_prompt_version: string,
    prepare_uri: string,
    prepare_at: string,
    sentiment_status: string,
    sentiment_model: string,
    sentiment_uri: string,
    sentiment_labeled_at: string,
    sentiment_prompt_version: string,
    embedding_status: string,
    embedding_model: string | null,
    embedding_uri: string | null,
    created_at: string,
    ready_at: string,
    prepare_llm_mode: string,
    sentiment_llm_mode: string,
    is_active: boolean,
}


export interface UploadDatasetPayload {
  file: string,
  data_type?: string,
  record_count?: string, // Integer string
  metadata?: string, /// JSON object string
  prepare_required?: string,
  prepare_llm_mode?: string, 
  sentiment_required?: string, 
  sentiment_llm_mode?: string,
  embedding_required?: string,
  prepare_model?: string,
  sentiment_model?: string,
  embedding_model?: string,
}