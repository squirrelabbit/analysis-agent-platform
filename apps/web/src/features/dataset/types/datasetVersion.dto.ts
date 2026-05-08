export interface UploadDatasetVersionRequest {
  file: File,
  metadata: Record<string, any>,
  data_type: "structured" |"unstructured",
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
  source_input_char_count: number;
  cleaned_input_char_count: number;
  clean_reduced_char_count: number;
}

export interface PrerpareSummaryDto {
  input_row_count: number;
  output_row_count: number;
  kept_count: number;
  review_count: number;
  dropped_count: number;
  text_column: string;
  text_columns: string[];
  text_joiner: string;
}

export interface ArtifactDto {
  artifact_id: string,
  project_id: string,
  dataset_id: string,
  dataset_version_id: string,
  artifact_type: string,
  stage: string,
  status: string,
  uri: string,
  format: string,
  metadata: any
  created_at: string
  updated_at: string
}

export interface ProgressDto {
  percent: number;
  processed_rows: number;
  total_rows: number;
  elapsed_seconds: number;
  message: string;
  updated_at: string
}

export interface DiagnosticsDto {
  retry_count: number,
  workflow_id: string,
  workflow_run_id: string,
  resumed_execution_count: number,
  progress?: ProgressDto
}

export interface BuildStageDto {
  stage: string;
  status: string;
  applicable: boolean;
  required: boolean;
  ready: boolean;
  depends_on: string[]
  can_run: boolean;
  run_group: string;
  auto_run_eligible: boolean;
  blocked_reason?:string;
  latest_job?: Record<string, any>,
  primary_artifact?: Record<string, any>,
  artifacts?: ArtifactDto[];
  summary?: Record<string, any>
  model?: string,
  prompt_version?:string,
  diagnostics?: DiagnosticsDto
}

export interface DatasetVersionResponse {
  dataset_version_id: string,
  dataset_id: string;
  project_id: string,
  metadata: any,
  storage_uri: string,
  data_type: string,
  record_count: number,
  source_summary: SourceSummaryDto
  build_stages: BuildStageDto[],
  is_active: boolean,
  clean_status: string,
  clean_summary?: CleanSummaryDto,
  prepare_status: string,
  prepare_summary?: PrerpareSummaryDto,
  sentiment_status: string,
  embedding_status: string,
}

export interface DatasetVersionListResponse {
  items: Omit<DatasetVersionResponse, "source_summary">[]
}