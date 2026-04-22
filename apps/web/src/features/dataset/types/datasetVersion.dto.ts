import type { LLMMode } from "./datasetVersion";

export interface DatasetVersionResponse {
  dataset_version_id: string;
  dataset_id: string;
  project_id: string;
  storage_uri: string;
  data_type: string;
  record_count: number;
  metadata: Record<string, any>;
  profile: Record<string, any>;
  prepare_status: string;
  prepare_llm_mode: string; // default
  prepare_model: string;
  prepare_prompt_version: string;
  prepare_uri: string;
  prepared_at: string;
  prepare_summary: Record<string, any>;
  sentiment_status: string;
  sentiment_llm_mode: string; // default
  sentiment_model: string;
  sentiment_uri: string;
  sentiment_labeled_at: string;
  sentiment_prompt_version: string;
  embedding_status: string;
  embedding_model: string;
  embedding_uri: string;
  is_active: boolean;
  created_at: string;
  ready_at: string;
}

export interface DatasetVersionListResponse {
  items: DatasetVersionResponse[];
}

export interface UploadDatasetVersionRequest {
  file: File;
  data_type?: string,
  record_count?: number,
  metadata?: Record<string, any>;
  activate_on_create?: boolean;
  prepare_required?: boolean;
  prepare_llm_mode?: LLMMode;
  sentiment_required?: boolean;
  sentiment_llm_mode?: LLMMode;
  embedding_required?: boolean;
  prepare_model?: string;
  sentiment_model?: string
  embedding_model?: string
}

export interface CreateDatasetVersionRequest {
  storage_uri: string;
  data_type: string;
  record_count: number;
  metadata: Record<string, any>;
  profile: Record<string, any>;
  // "profile": {
  //   "profile_id": "string",
  //   "prepare_prompt_version": "string",
  //   "sentiment_prompt_version": "string",
  //   "regex_rule_names": [
  //     "string"
  //   ],
  //   "garbage_rule_names": [
  //     "string"
  //   ],
  //   "embedding_model": "string"
  // },
  activate_on_create: boolean;
  prepare_required: boolean;
  prepare_llm_mode: LLMMode;
  prepare_model: string;
  sentiment_required: boolean;
  sentiment_llm_mode: LLMMode;
  sentiment_model: string;
  embedding_required: boolean;
  embedding_model: string;
}

export interface SampleResponse {
  source_row_index: number;
  row_id: string;
  raw_text: string;
  normalized_text: string;
  prepare_disposition: string;
  prepare_reason: string;
}

export interface PreparePreviewResponse {
  project_id: string;
  dataset_id: string;
  dataset_version_id: string;
  prepare_status: string;
  prepared_at: string;
  prepared_ref: string;
  prepare_format: string;
  raw_text_column: string;
  prepared_text_column: string;
  row_id_column: string;
  summary: Record<string, any>;
  sample_limit: number;
  samples: SampleResponse;
  warning_panel: {
    review_count: number;
    samples: SampleResponse[];
  };
}

export interface SentimentPreviewResponse {
  project_id: string;
  dataset_id: string;
  dataset_version_id: string;
  sentiment_status: string;
  sentiment_labeled_at: string;
  sentiment_ref: string;
  sentiment_format: string;
  sentiment_text_column: string;
  sentiment_label_column: string;
  sentiment_confidence_column: string;
  sentiment_reason_column: string;
  row_id_column: string;
  summary: Record<string, any>;
  sample_limit: number;
  samples: SampleResponse[];
}
