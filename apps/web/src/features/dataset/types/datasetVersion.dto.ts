export interface CleanJobPayload {
  text_columns?: string[];
  output_path?: string;
  preprocess_options?: {
    remove_english: boolean;
    remove_numbers: boolean;
    remove_special: boolean;
    remove_monosyllables: boolean;
  };
  force?: boolean;
}

export interface UploadDatasetVersionRequest {
  file: File,
  data_type: "structured" |"unstructured",
  activate_on_create: boolean;
}

export interface SourceSummaryDto {
  available: boolean;
  status: string; 
  format: string;
  row_count: number;
  column_count: number;
  columns: Record<string, any>[];
  sample_limit: number;
  sample_rows: Record<string, any>[];
  error_message?: string;
}

export interface CleanSummaryDto {
  input_row_count: number;
  output_row_count: number;
  kept_count: number;
  dropped_count: number;
  text_columns: string[];
  text_joiner: string;
  preprocess_options: {
    remove_english: false;
    remove_monosyllables: false;
    remove_numbers: false;
    remove_special: false;
  };
  source_input_char_count: number;
  cleaned_input_char_count: number;
  clean_reduced_char_count: number;
  clean_regex_rule_hits: {
    html_artifact: number;
    media_placeholder: number;
    url_cleanup: number;
  };
}

export interface DocGenuinenessSummaryDto {
  input_artifact_ref: string;
  input_row_count: number;
  model: string;
  parse_failures: number;
  processed_row_count: number;
  prompt_version: string;
  tier_counts: {
    genuine_review: number;
    mixed: number;
    non_review: number;
  };
  total_completion_tokens: number;
  total_prompt_tokens: number;
}

export interface ClauseLabelSummaryDto {
  // aspect_counts {
  //     "atmosphere": 0,
  //     "contents": 0,
  //     "convenience": 0,
  //     "etc": 0,
  //     "food": 0,
  //     "overall": 0,
  //     "value": 0
  // },
  clause_count: number;
  include_genuineness: string[];
  input_artifact_ref: string;
  input_row_count: number;
  model: string;
  parse_failures: number;
  processed_doc_count: number;
  prompt_version: string
  sentiment_counts: {
    negative: number;
    neutral: number;
    positive: number;
    mixed: number
  };
  // reasoning_effort
  skipped_by_filter: number;
  skipped_empty: number;
  total_completion_tokens: number;
  total_prompt_tokens: number;
}

export interface BuildStageResultDto<T = unknown> {
  status: string
  completed_at?: string
  summary?: T
}

export interface DatasetVersionResponse {
  dataset_version_id: string;
  created_at: string;
  is_active: boolean;
  row_count: number;
  column_count: number;
  columns: string[];
  byte_size: number;
  clean_status: string;
  doc_genuineness_status: string;
  clause_label_status: string;
  original_filename: string
}

export interface DatasetVersionListResponse {
  items: DatasetVersionResponse[]
}

export interface DatasetVersionDetailResponse {
  dataset_version_id: string;
  created_at: string;
  is_active: boolean;
  row_count: number;
  column_count: number;
  columns: string[];
  byte_size: number;
  clean: BuildStageResultDto;
  doc_genuineness: BuildStageResultDto;
  clause_label: BuildStageResultDto;
}