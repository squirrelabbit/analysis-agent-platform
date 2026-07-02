/**
 * GET /projects/{pid}/dataset_build_jobs/{job_id} 응답 계약 —
 * Go domain.DatasetBuildJob + BuildJobDiagnostics + BuildJobProgress와 필드·JSON 키 동일.
 */

export interface BuildJobProgressDto {
  percent: number;
  processed_rows: number;
  total_rows: number;
  elapsed_seconds?: number; // Go float64 omitempty — 0이면 생략
  eta_seconds?: number;
  message?: string;
  updated_at?: string;
}

export interface BuildJobDiagnosticsDto {
  retry_count: number;
  last_error_type?: string;
  last_error_message?: string;
  workflow_id?: string;
  workflow_run_id?: string;
  progress?: BuildJobProgressDto;
  llm_fallback?: boolean; // Go bool omitempty — false면 생략
  llm_fallback_reason?: string;
  llm_fallback_count?: number; // Go int omitempty — 0이면 생략
  llm_provider?: string;
  llm_model?: string;
  warnings?: string[];
}

export interface DatasetBuildJobDto {
  job_id: string;
  project_id: string;
  dataset_id: string;
  dataset_version_id: string;
  build_type: string;
  status: string;
  request?: Record<string, unknown>; // Go map omitempty — 빈 객체면 생략
  triggered_by?: string; // Go string omitempty — ''면 생략
  workflow_id?: string;
  workflow_run_id?: string;
  attempt: number;
  last_error_type?: string;
  created_at: string;
  started_at?: string;
  completed_at?: string;
  error_message?: string;
  diagnostics?: BuildJobDiagnosticsDto;
}
